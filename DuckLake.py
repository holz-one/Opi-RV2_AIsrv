#!/usr/bin/env python3
"""
DuckLake.py - DuckLake Analytical Lakehouse Engine & Storage Module
===================================================================
A standalone, modular lakehouse engine combining DuckDB in-memory analytical
compute with PostgreSQL metadata cataloging, Parquet storage (/srv/DuckLake),
raw warehouse dataset management (/srv/Warehouse), and Data Inlining.

Adheres to specifications from 'DuckLake: The Definitive Guide' (Chapters 1-6):
  - Catalog Layer (PostgreSQL: snapshots, changes, tables, columns, inlined_data, options)
  - Storage Layer (Standard Parquet, ZSTD, append-only, Hive partitioning)
  - Compute Layer (DuckDB engine with vector projection, filter pushdown, pruning)
  - Data Inlining (automatic catalog buffering for batches <= DATA_INLINING_ROW_LIMIT)
  - Compaction & Maintenance (merge_adjacent_files, flush_inlined_data, cleanup_old_files)
"""

import decimal
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
import pandas as pd
import psycopg
from werkzeug.utils import secure_filename

logger = logging.getLogger("DuckLake")

_MODULE_CONFIG: Dict[str, Any] = {
    "storage": {
        "ducklake_dir": "/srv/DuckLake",
        "warehouse_dir": "/srv/Warehouse",
        "db_cache_dir": "/cluster/cache",
    },
    "postgres": {
        "dbname": "orangepi",
        "user": "orangepi",
        "host": "",
        "port": 5432,
        "password": "",
        "connect_timeout": 10,
    },
    "ducklake": {
        "memory_limit": "1GB",
        "threads": 4,
        "auto_scan_on_startup": True,
        "auto_create_views": True,
        "max_query_rows": 5000,
    },
}


def set_config(config: Dict[str, Any]):
    """Updates the module-level configuration dictionary."""
    global _MODULE_CONFIG
    _MODULE_CONFIG.update(config)


def get_pg_connection(config: Optional[Dict[str, Any]] = None):
    """Establishes a connection to the PostgreSQL metadata database."""
    cfg = config or _MODULE_CONFIG
    pg = cfg.get("postgres", {})
    conn_params = {
        "dbname": pg.get("dbname", "orangepi"),
        "user": pg.get("user", "orangepi"),
        "connect_timeout": pg.get("connect_timeout", 10),
    }
    host = pg.get("host", "").strip() if pg.get("host") else ""
    if host:
        conn_params["host"] = host
        conn_params["port"] = pg.get("port", 5432)
    if pg.get("password"):
        conn_params["password"] = pg["password"]
    return psycopg.connect(**conn_params)


def resolve_safe_path(base_dir: Path, target_relative: str) -> Optional[Path]:
    """Ensures target path stays strictly inside base_dir to prevent directory traversal."""
    try:
        resolved_base = base_dir.resolve()
        target_path = (resolved_base / target_relative).resolve()
        if os.path.commonpath([str(resolved_base), str(target_path)]) == str(resolved_base):
            return target_path
    except Exception:
        pass
    return None


def init_postgres_metadata(config: Optional[Dict[str, Any]] = None):
    """Initializes metadata tables and views in PostgreSQL for DuckLake, Warehouse, and AI."""
    cfg = config or _MODULE_CONFIG
    try:
        with get_pg_connection(cfg) as conn:
            with conn.cursor() as cur:
                # 0. DuckLake Projects Partition Catalog
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ducklake_projects (
                        project_id VARCHAR(64) PRIMARY KEY,
                        display_name VARCHAR(128) NOT NULL,
                        description TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # 1. DuckLake Parquet Tables Catalog
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ducklake_tables (
                        table_name VARCHAR(128) PRIMARY KEY,
                        project_id VARCHAR(64) DEFAULT 'default',
                        source_path TEXT NOT NULL,
                        format VARCHAR(32) DEFAULT 'parquet',
                        row_count BIGINT DEFAULT 0,
                        file_size_bytes BIGINT DEFAULT 0,
                        schema_info JSONB DEFAULT '[]'::jsonb,
                        description TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    ALTER TABLE ducklake_tables ADD COLUMN IF NOT EXISTS project_id VARCHAR(64) DEFAULT 'default';
                """)

                # 2. Raw Warehouse Inventory Catalog
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ducklake_raw_files (
                        file_path TEXT PRIMARY KEY,
                        project_id VARCHAR(64) DEFAULT 'default',
                        file_name VARCHAR(255) NOT NULL,
                        file_format VARCHAR(32) NOT NULL,
                        file_size_bytes BIGINT DEFAULT 0,
                        status VARCHAR(32) DEFAULT 'raw',
                        schema_info JSONB DEFAULT '[]'::jsonb,
                        row_count BIGINT,
                        detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        last_scanned TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    ALTER TABLE ducklake_raw_files ADD COLUMN IF NOT EXISTS project_id VARCHAR(64) DEFAULT 'default';
                    ALTER TABLE ducklake_raw_files ADD COLUMN IF NOT EXISTS relative_path TEXT;
                    ALTER TABLE ducklake_raw_files ADD COLUMN IF NOT EXISTS category VARCHAR(128) DEFAULT 'general';
                    CREATE INDEX IF NOT EXISTS idx_ducklake_raw_files_category ON ducklake_raw_files(category);
                    CREATE INDEX IF NOT EXISTS idx_ducklake_raw_files_format ON ducklake_raw_files(file_format);
                    CREATE INDEX IF NOT EXISTS idx_ducklake_raw_files_relpath ON ducklake_raw_files(relative_path);
                """)

                # Seed default project partition
                cur.execute("""
                    INSERT INTO ducklake_projects (project_id, display_name, description)
                    VALUES ('default', 'Default Lake', 'Root partition for standalone tables and general raw warehouse files')
                    ON CONFLICT (project_id) DO NOTHING;
                """)

                # 3. AI Models Registry
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_models (
                        id VARCHAR(255) PRIMARY KEY,
                        owned_by VARCHAR(128),
                        directory_path TEXT NOT NULL,
                        variant_name VARCHAR(128),
                        model_type VARCHAR(64),
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # 4. AI Chat Logs
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ai_chat_logs (
                        id BIGSERIAL PRIMARY KEY,
                        model_id VARCHAR(255),
                        role VARCHAR(32),
                        content TEXT,
                        client_ip VARCHAR(64),
                        prompt_tokens INTEGER DEFAULT 0,
                        completion_tokens INTEGER DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # 5. DuckLake SQL Query History
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ducklake_queries (
                        id BIGSERIAL PRIMARY KEY,
                        query_text TEXT NOT NULL,
                        client_ip VARCHAR(64),
                        execution_time_ms DOUBLE PRECISION,
                        row_count BIGINT,
                        status VARCHAR(32),
                        error_message TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # 6. Backward Compatibility Views
                cur.execute("""
                    CREATE OR REPLACE VIEW models AS
                    SELECT id, owned_by, directory_path, variant_name, created_at FROM ai_models;
                """)
                cur.execute("""
                    CREATE OR REPLACE VIEW chat_logs AS
                    SELECT id::text AS log_id, model_id, role, content, created_at AS timestamp FROM ai_chat_logs;
                """)

                # 7. Canonical DuckLake Lakehouse Catalog Tables (per O'Reilly DuckLake Guide)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ducklake_snapshot (
                        snapshot_id BIGSERIAL PRIMARY KEY,
                        snapshot_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        schema_version INT DEFAULT 1
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
                        id BIGSERIAL PRIMARY KEY,
                        snapshot_id BIGINT,
                        schema_version INT DEFAULT 1,
                        operation VARCHAR(64) NOT NULL,
                        description TEXT DEFAULT '',
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_table (
                        table_id BIGSERIAL PRIMARY KEY,
                        table_name VARCHAR(128) NOT NULL,
                        schema_name VARCHAR(64) DEFAULT 'main',
                        project_id VARCHAR(64) DEFAULT 'default',
                        path TEXT NOT NULL,
                        format VARCHAR(32) DEFAULT 'parquet',
                        begin_snapshot BIGINT DEFAULT 1,
                        end_snapshot BIGINT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (table_name, project_id)
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_column (
                        column_id BIGSERIAL PRIMARY KEY,
                        table_id BIGINT REFERENCES ducklake_table(table_id) ON DELETE CASCADE,
                        column_name VARCHAR(128) NOT NULL,
                        column_type VARCHAR(64) NOT NULL,
                        ordinal_position INT DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_data_file (
                        data_file_id BIGSERIAL PRIMARY KEY,
                        table_id BIGINT REFERENCES ducklake_table(table_id) ON DELETE CASCADE,
                        path TEXT NOT NULL,
                        file_format VARCHAR(32) DEFAULT 'parquet',
                        record_count BIGINT DEFAULT 0,
                        file_size_bytes BIGINT DEFAULT 0,
                        begin_snapshot BIGINT DEFAULT 1,
                        end_snapshot BIGINT,
                        encryption_key TEXT
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_table_stats (
                        table_id BIGINT PRIMARY KEY REFERENCES ducklake_table(table_id) ON DELETE CASCADE,
                        record_count BIGINT DEFAULT 0,
                        file_count INT DEFAULT 0,
                        total_size_bytes BIGINT DEFAULT 0,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (
                        data_file_id BIGINT REFERENCES ducklake_data_file(data_file_id) ON DELETE CASCADE,
                        column_id BIGINT REFERENCES ducklake_column(column_id) ON DELETE CASCADE,
                        column_size_bytes BIGINT DEFAULT 0,
                        value_count BIGINT DEFAULT 0,
                        null_count BIGINT DEFAULT 0,
                        min_value TEXT,
                        max_value TEXT,
                        PRIMARY KEY (data_file_id, column_id)
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_tag (
                        table_id BIGINT REFERENCES ducklake_table(table_id) ON DELETE CASCADE,
                        tag_name VARCHAR(64) NOT NULL,
                        tag_value TEXT NOT NULL,
                        PRIMARY KEY (table_id, tag_name)
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_column_tag (
                        column_id BIGINT REFERENCES ducklake_column(column_id) ON DELETE CASCADE,
                        tag_name VARCHAR(64) NOT NULL,
                        tag_value TEXT NOT NULL,
                        PRIMARY KEY (column_id, tag_name)
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_options (
                        option_name VARCHAR(64) PRIMARY KEY,
                        option_value TEXT NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS ducklake_inlined_data (
                        inlined_id BIGSERIAL PRIMARY KEY,
                        table_name VARCHAR(128) NOT NULL,
                        project_id VARCHAR(64) DEFAULT 'default',
                        row_data JSONB NOT NULL,
                        inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Seed initial snapshot and default options if empty
                cur.execute("""
                    INSERT INTO ducklake_snapshot (snapshot_id, schema_version)
                    VALUES (1, 1)
                    ON CONFLICT (snapshot_id) DO NOTHING;

                    INSERT INTO ducklake_snapshot_changes (snapshot_id, operation, description)
                    VALUES (1, 'INIT', 'Initial DuckLake catalog snapshot')
                    ON CONFLICT DO NOTHING;

                    INSERT INTO ducklake_options (option_name, option_value) VALUES
                        ('data_inlining_row_limit', '10'),
                        ('parquet_compression', 'zstd'),
                        ('parquet_version', '1.0'),
                        ('target_file_size', '512MB'),
                        ('parquet_row_group_size', '122880'),
                        ('expire_older_than', '1 month'),
                        ('hive_file_pattern', 'true')
                    ON CONFLICT (option_name) DO NOTHING;
                """)

                conn.commit()
                logger.info("PostgreSQL DuckLake catalog schemas verified successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL metadata: {e}")


class DuckLakeEngine:
    """
    Core DuckLake Lakehouse Engine.
    Combines DuckDB analytical engine with PostgreSQL metadata cataloging,
    Parquet persistence, and Data Inlining buffers.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        set_config(config)
        self.ducklake_dir = Path(config["storage"]["ducklake_dir"]).resolve()
        self.warehouse_dir = Path(config["storage"]["warehouse_dir"]).resolve()
        self.ducklake_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(database=":memory:")
        self._apply_pragmas()
        if self.config.get("ducklake", {}).get("auto_scan_on_startup", True):
            try:
                self.scan_and_sync()
            except Exception as e:
                logger.warning(f"Initial DuckLake storage scan failed: {e}")

    def _apply_pragmas(self):
        mem_limit = self.config.get("ducklake", {}).get("memory_limit", "1GB")
        threads = self.config.get("ducklake", {}).get("threads", 4)
        try:
            self.conn.execute(f"PRAGMA memory_limit='{mem_limit}';")
            self.conn.execute(f"PRAGMA threads={threads};")
            logger.info(f"DuckLake Engine initialized (memory_limit={mem_limit}, threads={threads})")
        except Exception as e:
            logger.warning(f"Failed to set DuckDB pragmas: {e}")

    def get_pg_connection(self):
        """Helper to get a PostgreSQL connection using the engine's configuration."""
        return get_pg_connection(self.config)

    @staticmethod
    def sanitize_table_name(name: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip().lower())
        clean = re.sub(r"_+", "_", clean).strip("_")
        if not clean or clean[0].isdigit():
            clean = f"t_{clean}"
        return clean[:63]

    def get_option(self, option_name: str, default: Any = None) -> Any:
        """Retrieves a DuckLake configuration parameter from PostgreSQL catalog."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT option_value FROM ducklake_options WHERE option_name = %s;", (option_name,))
                    row = cur.fetchone()
                    if row:
                        return row[0]
        except Exception:
            pass
        return default

    def set_option(self, option_name: str, option_value: Any):
        """Sets a DuckLake configuration parameter in PostgreSQL catalog (Chapter 5: CALL set_option)."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO ducklake_options (option_name, option_value, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (option_name) DO UPDATE SET option_value = EXCLUDED.option_value, updated_at = CURRENT_TIMESTAMP;
                    """, (option_name, str(option_value)))
                    conn.commit()
        except Exception as e:
            logger.warning(f"Failed to set DuckLake option {option_name}: {e}")

    def create_snapshot(self, operation: str, description: str = "") -> int:
        """Creates a new transactional snapshot in the DuckLake catalog (Chapter 4)."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, 1) RETURNING snapshot_id;")
                    snap_id = cur.fetchone()[0]
                    cur.execute("""
                        INSERT INTO ducklake_snapshot_changes (snapshot_id, schema_version, operation, description, timestamp)
                        VALUES (%s, 1, %s, %s, CURRENT_TIMESTAMP);
                    """, (snap_id, operation, description))
                    conn.commit()
                    return snap_id
        except Exception as e:
            logger.warning(f"Failed to record DuckLake snapshot: {e}")
            return 1

    def get_snapshots(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieves DuckLake catalog snapshot transaction history (Chapter 4: FROM snapshots())."""
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT s.snapshot_id, s.snapshot_time, s.schema_version, 
                               COALESCE(sc.operation, 'SNAPSHOT') AS operation, 
                               COALESCE(sc.description, '') AS description
                        FROM ducklake_snapshot s
                        LEFT JOIN ducklake_snapshot_changes sc ON s.snapshot_id = sc.snapshot_id
                        ORDER BY s.snapshot_id DESC LIMIT %s;
                    """, (limit,))
                    rows = cur.fetchall()
                    return [
                        {
                            "snapshot_id": r[0],
                            "snapshot_time": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
                            "schema_version": r[2],
                            "operation": r[3],
                            "description": r[4]
                        }
                        for r in rows
                    ]
        except Exception as e:
            logger.warning(f"Failed to fetch DuckLake snapshots: {e}")
            return []

    def scan_and_sync(self) -> Dict[str, Any]:
        """
        Scans /srv/DuckLake for Parquet files and /srv/Warehouse for raw datasets.
        Registers schemas, project partitions, and metadata in PostgreSQL, and creates views in DuckDB.
        """
        stats = {"parquet_tables": 0, "warehouse_files": 0, "projects": 0, "errors": []}
        
        # 1. Scan Parquet Files in DuckLake
        parquet_files = list(self.ducklake_dir.rglob("*.parquet"))
        for pfile in parquet_files:
            try:
                rel_parts = pfile.relative_to(self.ducklake_dir).with_suffix("").parts
                if len(rel_parts) == 1:
                    proj_id = "default"
                    table_name = self.sanitize_table_name(rel_parts[0])
                    short_name = table_name
                else:
                    proj_id = self.sanitize_table_name(rel_parts[0])
                    table_name = self.sanitize_table_name("_".join(rel_parts))
                    short_name = self.sanitize_table_name(f"{rel_parts[-2]}_{rel_parts[-1]}")
                p_str = str(pfile.resolve())
                
                # Extract schema & row count using DuckDB
                schema_rows = self.conn.execute(f"DESCRIBE SELECT * FROM parquet_scan('{p_str}') LIMIT 0").fetchall()
                schema_info = [{"name": row[0], "type": row[1]} for row in schema_rows]
                
                row_count_res = self.conn.execute(f"SELECT count(*) FROM parquet_scan('{p_str}')").fetchone()
                row_count = row_count_res[0] if row_count_res else 0
                file_size = pfile.stat().st_size

                # Register project and table in PostgreSQL
                with self.get_pg_connection() as pg_conn:
                    with pg_conn.cursor() as cur:
                        if proj_id != "default":
                            cur.execute("""
                                INSERT INTO ducklake_projects (project_id, display_name, description)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (project_id) DO NOTHING;
                            """, (proj_id, rel_parts[0], f"Project partition for {rel_parts[0]}"))

                        cur.execute("""
                            INSERT INTO ducklake_tables (table_name, project_id, source_path, format, row_count, file_size_bytes, schema_info, updated_at)
                            VALUES (%s, %s, %s, 'parquet', %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (table_name) DO UPDATE SET
                                project_id = EXCLUDED.project_id,
                                source_path = EXCLUDED.source_path,
                                row_count = EXCLUDED.row_count,
                                file_size_bytes = EXCLUDED.file_size_bytes,
                                schema_info = EXCLUDED.schema_info,
                                updated_at = CURRENT_TIMESTAMP;
                        """, (table_name, proj_id, p_str, row_count, file_size, json.dumps(schema_info)))

                        # Canonical DuckLake Guide catalog tables
                        cur.execute("""
                            INSERT INTO ducklake_table (table_name, schema_name, project_id, path, format, begin_snapshot)
                            VALUES (%s, 'main', %s, %s, 'parquet', 1)
                            ON CONFLICT (table_name, project_id) DO UPDATE SET
                                path = EXCLUDED.path,
                                format = EXCLUDED.format
                            RETURNING table_id;
                        """, (table_name, proj_id, p_str))
                        t_row = cur.fetchone()
                        t_id = t_row[0] if t_row else None

                        if t_id:
                            for idx, col in enumerate(schema_info):
                                cur.execute("""
                                    INSERT INTO ducklake_column (table_id, column_name, column_type, ordinal_position)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING;
                                """, (t_id, col["name"], col["type"], idx))

                            cur.execute("""
                                INSERT INTO ducklake_data_file (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot)
                                VALUES (%s, %s, 'parquet', %s, %s, 1)
                                ON CONFLICT DO NOTHING;
                            """, (t_id, p_str, row_count, file_size))

                            cur.execute("""
                                INSERT INTO ducklake_table_stats (table_id, record_count, file_count, total_size_bytes, updated_at)
                                VALUES (%s, %s, 1, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT (table_id) DO UPDATE SET
                                    record_count = EXCLUDED.record_count,
                                    file_count = 1,
                                    total_size_bytes = EXCLUDED.total_size_bytes,
                                    updated_at = CURRENT_TIMESTAMP;
                            """, (t_id, row_count, file_size))

                        # Check for inlined data records for this table (Chapter 3)
                        cur.execute("SELECT row_data FROM ducklake_inlined_data WHERE table_name = %s AND project_id = %s;", (table_name, proj_id))
                        i_rows = cur.fetchall()

                        pg_conn.commit()

                # Expose view in DuckDB for instant SQL querying (uniting Parquet and inlined buffer if present)
                if self.config.get("ducklake", {}).get("auto_create_views", True):
                    if i_rows:
                        i_df = pd.DataFrame([r[0] for r in i_rows])
                        self.conn.register(f"_inlined_{table_name}", i_df)
                        self.conn.execute(f'''
                            CREATE OR REPLACE VIEW "{table_name}" AS
                            SELECT * FROM parquet_scan('{p_str}')
                            UNION ALL BY NAME
                            SELECT * FROM "_inlined_{table_name}";
                        ''')
                    else:
                        self.conn.execute(f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM parquet_scan(\'{p_str}\');')
                    if short_name != table_name:
                        self.conn.execute(f'CREATE OR REPLACE VIEW "{short_name}" AS SELECT * FROM "{table_name}";')

                stats["parquet_tables"] += 1
            except Exception as e:
                err_msg = f"Failed to register parquet file {pfile}: {e}"
                logger.error(err_msg)
                stats["errors"].append(err_msg)

        # 2. Scan Raw Warehouse Files Recursively (all formats and categories)
        existing_wh_paths = set()
        for root, dirs, files in os.walk(str(self.warehouse_dir)):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for f in files:
                if f.startswith(".") and f != ".env":
                    continue
                if f.endswith(".tmp") or f.endswith(".part"):
                    continue
                f_path = Path(root) / f
                if not f_path.is_file():
                    continue

                try:
                    f_str = str(f_path.resolve())
                    existing_wh_paths.add(f_str)
                    f_size = f_path.stat().st_size
                    rel_p = f_path.relative_to(self.warehouse_dir)
                    rel_parts = rel_p.parts
                    rel_path_str = str(rel_p)

                    # Determine category and project partition
                    if len(rel_parts) > 1:
                        top_folder = rel_parts[0]
                        category = top_folder
                        proj_id = self.sanitize_table_name(top_folder)
                        proj_name = top_folder
                    else:
                        category = "general"
                        proj_id = "default"
                        proj_name = "Default Lake"

                    ext = f_path.suffix.lower()
                    f_format = ext.lstrip(".") if ext else "unknown"
                    if not f_format:
                        f_format = "unknown"
                    schema_info = []

                    # Sample CSV / JSON schemas via DuckDB where possible
                    if ext in {".csv", ".tsv"}:
                        try:
                            d_schema = self.conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{f_str}') LIMIT 0").fetchall()
                            schema_info = [{"name": r[0], "type": r[1]} for r in d_schema]
                            raw_view_name = f"raw_{self.sanitize_table_name(f_path.stem)}"
                            self.conn.execute(f'CREATE OR REPLACE VIEW "{raw_view_name}" AS SELECT * FROM read_csv_auto(\'{f_str}\');')
                        except Exception:
                            pass
                    elif ext in {".json", ".jsonl"}:
                        try:
                            d_schema = self.conn.execute(f"DESCRIBE SELECT * FROM read_json_auto('{f_str}') LIMIT 0").fetchall()
                            schema_info = [{"name": r[0], "type": r[1]} for r in d_schema]
                            raw_view_name = f"raw_{self.sanitize_table_name(f_path.stem)}"
                            self.conn.execute(f'CREATE OR REPLACE VIEW "{raw_view_name}" AS SELECT * FROM read_json_auto(\'{f_str}\');')
                        except Exception:
                            pass

                    with self.get_pg_connection() as pg_conn:
                        with pg_conn.cursor() as cur:
                            if proj_id != "default":
                                cur.execute("""
                                    INSERT INTO ducklake_projects (project_id, display_name, description)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (project_id) DO NOTHING;
                                """, (proj_id, proj_name, f"Project partition for {proj_name}"))

                            cur.execute("""
                                INSERT INTO ducklake_raw_files (
                                    file_path, project_id, file_name, relative_path, category,
                                    file_format, file_size_bytes, schema_info, last_scanned
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT (file_path) DO UPDATE SET
                                    project_id = EXCLUDED.project_id,
                                    file_name = EXCLUDED.file_name,
                                    relative_path = EXCLUDED.relative_path,
                                    category = EXCLUDED.category,
                                    file_format = EXCLUDED.file_format,
                                    file_size_bytes = EXCLUDED.file_size_bytes,
                                    schema_info = EXCLUDED.schema_info,
                                    last_scanned = CURRENT_TIMESTAMP;
                            """, (f_str, proj_id, f, rel_path_str, category, f_format, f_size, json.dumps(schema_info)))
                            pg_conn.commit()

                    stats["warehouse_files"] += 1
                except Exception as e:
                    err_msg = f"Failed to catalog raw file {f_path}: {e}"
                    logger.error(err_msg)
                    stats["errors"].append(err_msg)

        # Prune raw files deleted from disk
        try:
            with self.get_pg_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute("SELECT file_path FROM ducklake_raw_files;")
                    db_paths = [r[0] for r in cur.fetchall()]
                    deleted_paths = [p for p in db_paths if p not in existing_wh_paths]
                    if deleted_paths:
                        cur.execute("DELETE FROM ducklake_raw_files WHERE file_path = ANY(%s);", (deleted_paths,))
                        pg_conn.commit()
                        logger.info(f"Pruned {len(deleted_paths)} deleted warehouse files from catalog.")
        except Exception as prune_err:
            logger.warning(f"Failed to prune deleted warehouse files: {prune_err}")

        # 3. Expose Canonical DuckLake Metadata Views in DuckDB (Chapter 4: FROM snapshots())
        try:
            with self.get_pg_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    # snapshots view
                    cur.execute("""
                        SELECT s.snapshot_id, s.snapshot_time, s.schema_version, 
                               COALESCE(sc.operation, 'SNAPSHOT') AS operation, 
                               COALESCE(sc.description, '') AS description
                        FROM ducklake_snapshot s
                        LEFT JOIN ducklake_snapshot_changes sc ON s.snapshot_id = sc.snapshot_id
                        ORDER BY s.snapshot_id DESC LIMIT 100;
                    """)
                    snaps = cur.fetchall()
                    if snaps:
                        snap_df = pd.DataFrame(snaps, columns=["snapshot_id", "snapshot_time", "schema_version", "operation", "description"])
                        self.conn.register("_snapshots_raw", snap_df)
                        self.conn.execute('CREATE OR REPLACE VIEW snapshots AS SELECT * FROM _snapshots_raw;')

                    # ducklake_table view
                    cur.execute("SELECT table_id, table_name, schema_name, project_id, path, format FROM ducklake_table;")
                    t_rows = cur.fetchall()
                    if t_rows:
                        t_df = pd.DataFrame(t_rows, columns=["table_id", "table_name", "schema_name", "project_id", "path", "format"])
                        self.conn.register("_dtables_raw", t_df)
                        self.conn.execute('CREATE OR REPLACE VIEW ducklake_table AS SELECT * FROM _dtables_raw;')

                    # Inlined-only tables
                    cur.execute("""
                        SELECT DISTINCT table_name, project_id 
                        FROM ducklake_inlined_data 
                        WHERE table_name NOT IN (SELECT table_name FROM ducklake_tables);
                    """)
                    for t_name, p_id in cur.fetchall():
                        cur.execute("SELECT row_data FROM ducklake_inlined_data WHERE table_name = %s AND project_id = %s;", (t_name, p_id))
                        in_records = cur.fetchall()
                        if in_records:
                            df_in = pd.DataFrame([r[0] for r in in_records])
                            self.conn.register(f"_inlined_{t_name}", df_in)
                            self.conn.execute(f'CREATE OR REPLACE VIEW "{t_name}" AS SELECT * FROM "_inlined_{t_name}";')
        except Exception as e:
            logger.warning(f"Failed to register DuckLake catalog views in DuckDB: {e}")

        logger.info(f"DuckLake scan completed: {stats['parquet_tables']} parquet tables, {stats['warehouse_files']} raw warehouse files.")
        return stats

    def execute_query(self, sql_query: str, client_ip: str = "127.0.0.1") -> Dict[str, Any]:
        """Executes analytical SQL query on DuckDB and records telemetry in PostgreSQL."""
        start_time = time.time()
        max_rows = self.config.get("ducklake", {}).get("max_query_rows", 5000)
        
        try:
            rel = self.conn.execute(sql_query)
            cols = [desc[0] for desc in rel.description] if rel.description else []
            col_types = [str(desc[1]) for desc in rel.description] if rel.description else []
            
            raw_rows = rel.fetchmany(max_rows)
            rows = []
            for r in raw_rows:
                formatted_row = []
                for val in r:
                    if hasattr(val, "isoformat"):
                        formatted_row.append(val.isoformat())
                    elif isinstance(val, decimal.Decimal):
                        formatted_row.append(float(val))
                    elif isinstance(val, (bytes, bytearray)):
                        formatted_row.append(f"<blob {len(val)} bytes>")
                    else:
                        formatted_row.append(val)
                rows.append(formatted_row)

            duration_ms = round((time.time() - start_time) * 1000, 2)
            row_count = len(rows)

            try:
                with self.get_pg_connection() as pg_conn:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO ducklake_queries (query_text, client_ip, execution_time_ms, row_count, status)
                            VALUES (%s, %s, %s, %s, 'success');
                        """, (sql_query, client_ip, duration_ms, row_count))
                        pg_conn.commit()
            except Exception as log_err:
                logger.warning(f"Could not record query telemetry in PostgreSQL: {log_err}")

            return {
                "status": "success",
                "columns": cols,
                "types": col_types,
                "rows": rows,
                "row_count": row_count,
                "execution_time_ms": duration_ms,
            }
        except Exception as e:
            duration_ms = round((time.time() - start_time) * 1000, 2)
            err_text = str(e)
            try:
                with self.get_pg_connection() as pg_conn:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO ducklake_queries (query_text, client_ip, execution_time_ms, status, error_message)
                            VALUES (%s, %s, %s, 'error', %s);
                        """, (sql_query, client_ip, duration_ms, err_text))
                        pg_conn.commit()
            except Exception:
                pass
            return {
                "status": "error",
                "error": err_text,
                "execution_time_ms": duration_ms,
            }

    def convert_raw_to_parquet(self, raw_filename: str, target_table: Optional[str] = None) -> Dict[str, Any]:
        """Converts a raw warehouse file (CSV/JSON) into high-performance Parquet in /srv/DuckLake."""
        source_path = resolve_safe_path(self.warehouse_dir, raw_filename)
        if not source_path or not source_path.exists():
            matches = list(self.warehouse_dir.rglob(raw_filename))
            if matches and matches[0].is_file():
                source_path = matches[0]
            else:
                return {"status": "error", "error": f"File {raw_filename} not found in {self.warehouse_dir}"}

        table_name = self.sanitize_table_name(target_table or source_path.stem)
        target_parquet = self.ducklake_dir / f"{table_name}.parquet"
        
        ext = source_path.suffix.lower()
        if ext in {".csv", ".tsv"}:
            read_expr = f"read_csv_auto('{source_path.resolve()}')"
        elif ext in {".json", ".jsonl"}:
            read_expr = f"read_json_auto('{source_path.resolve()}')"
        elif ext in {".parquet", ".pq"}:
            shutil.copy2(source_path, target_parquet)
            self.scan_and_sync()
            return {"status": "success", "table_name": table_name, "target_file": str(target_parquet)}
        else:
            return {"status": "error", "error": f"Direct conversion for format {ext} not supported. Use pg_import.py."}

        copy_sql = f"COPY (SELECT * FROM {read_expr}) TO '{target_parquet.resolve()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        try:
            self.conn.execute(copy_sql)
            with self.get_pg_connection() as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ducklake_raw_files SET status = 'converted_to_parquet' WHERE file_path = %s;
                    """, (str(source_path.resolve()),))
                    pg_conn.commit()
            self.scan_and_sync()
            return {"status": "success", "table_name": table_name, "target_file": str(target_parquet)}
        except Exception as e:
            return {"status": "error", "error": f"Conversion failed: {e}"}

    def insert_records(self, table_name: str, records: List[Dict[str, Any]], mode: str = "append", project_id: Optional[str] = None) -> Dict[str, Any]:
        """Inserts JSON records into a Parquet table in /srv/DuckLake with Data Inlining (Chapter 3)."""
        if not records or not isinstance(records, list):
            return {"status": "error", "error": "Records must be a non-empty list of JSON objects"}

        sanitized_table = self.sanitize_table_name(table_name)
        proj_slug = self.sanitize_table_name(project_id) if project_id and project_id != "default" else None
        if proj_slug:
            target_dir = self.ducklake_dir / proj_slug
            target_dir.mkdir(parents=True, exist_ok=True)
            target_parquet = target_dir / f"{sanitized_table}.parquet"
        else:
            target_parquet = self.ducklake_dir / f"{sanitized_table}.parquet"

        try:
            inlining_limit = int(self.get_option("data_inlining_row_limit", 10))
            p_str = str(target_parquet.resolve())

            # Data Inlining optimization: store small append batches in catalog database
            if mode == "append" and len(records) <= inlining_limit:
                with self.get_pg_connection() as conn:
                    with conn.cursor() as cur:
                        for rec in records:
                            cur.execute("""
                                INSERT INTO ducklake_inlined_data (table_name, project_id, row_data)
                                VALUES (%s, %s, %s);
                            """, (sanitized_table, proj_slug or "default", json.dumps(rec)))
                        conn.commit()
                snap_id = self.create_snapshot("INLINE_INSERT", f"Inlined {len(records)} rows into '{sanitized_table}'")
                self.scan_and_sync()
                return {
                    "status": "success",
                    "inlined": True,
                    "inlined_rows": len(records),
                    "table_name": sanitized_table,
                    "project_id": proj_slug or "default",
                    "mode": mode,
                    "snapshot_id": snap_id
                }

            # If writing directly to Parquet (large batch or replace mode):
            new_df = pd.DataFrame(records)
            self.conn.register("_new_records_df", new_df)

            prior_inlined = []
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT inlined_id, row_data FROM ducklake_inlined_data WHERE table_name = %s AND project_id = %s;", (sanitized_table, proj_slug or "default"))
                    prior_inlined = cur.fetchall()

            if prior_inlined and mode == "append":
                inlined_df = pd.DataFrame([r[1] for r in prior_inlined])
                self.conn.register("_prior_inlined_df", inlined_df)
                inlined_clause = "UNION ALL BY NAME SELECT * FROM _prior_inlined_df"
            else:
                inlined_clause = ""

            if mode == "append" and target_parquet.exists():
                query = f"""
                    CREATE OR REPLACE TABLE _combined_batch AS
                    SELECT * FROM parquet_scan('{p_str}')
                    {inlined_clause}
                    UNION ALL BY NAME
                    SELECT * FROM _new_records_df;
                """
                self.conn.execute(query)
                self.conn.execute(f"COPY _combined_batch TO '{p_str}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);")
                self.conn.execute("DROP TABLE IF EXISTS _combined_batch;")
            else:
                query = "CREATE OR REPLACE TABLE _combined_batch AS SELECT * FROM _new_records_df;"
                self.conn.execute(query)
                self.conn.execute(f"COPY _combined_batch TO '{p_str}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);")
                self.conn.execute("DROP TABLE IF EXISTS _combined_batch;")

            self.conn.unregister("_new_records_df")
            if prior_inlined and mode == "append":
                self.conn.unregister("_prior_inlined_df")
                with self.get_pg_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM ducklake_inlined_data WHERE inlined_id = ANY(%s);", ([r[0] for r in prior_inlined],))
                        conn.commit()

            snap_op = "REPLACE" if mode != "append" else "INSERT"
            snap_id = self.create_snapshot(snap_op, f"{snap_op} {len(records)} rows into '{sanitized_table}'")
            self.scan_and_sync()

            total_rows = self.conn.execute(f"SELECT count(*) FROM parquet_scan('{p_str}')").fetchone()[0]
            return {
                "status": "success",
                "inlined": False,
                "table_name": sanitized_table,
                "project_id": proj_slug or "default",
                "mode": mode,
                "rows_inserted": len(records),
                "total_rows": total_rows,
                "file_path": p_str,
                "file_size_bytes": target_parquet.stat().st_size,
                "snapshot_id": snap_id
            }
        except Exception as e:
            logger.error(f"Failed to insert records into table {sanitized_table}: {e}")
            return {"status": "error", "error": str(e)}

    def ingest_file_to_ducklake(self, file_storage, filename: str, table_name: Optional[str] = None, mode: str = "replace", project_id: Optional[str] = None) -> Dict[str, Any]:
        """Ingests an uploaded file (Parquet, CSV, TSV, JSON) into /srv/DuckLake (optionally partitioned by project)."""
        sec_filename = secure_filename(filename)
        stem = Path(sec_filename).stem
        sanitized_table = self.sanitize_table_name(table_name or stem)
        proj_slug = self.sanitize_table_name(project_id) if project_id and project_id != "default" else None

        if proj_slug:
            target_dir = self.ducklake_dir / proj_slug
            target_dir.mkdir(parents=True, exist_ok=True)
            target_parquet = target_dir / f"{sanitized_table}.parquet"
        else:
            target_parquet = self.ducklake_dir / f"{sanitized_table}.parquet"

        ext = Path(sec_filename).suffix.lower()
        allowed_exts = {".parquet", ".pq", ".csv", ".tsv", ".json", ".jsonl"}
        if ext not in allowed_exts:
            return {"status": "error", "error": f"Unsupported format '{ext}'. Allowed formats: {sorted(list(allowed_exts))}"}

        temp_fd, temp_path = tempfile.mkstemp(prefix="upload_ducklake_", suffix=ext)
        os.close(temp_fd)
        try:
            file_storage.save(temp_path)
            temp_path_str = str(Path(temp_path).resolve())
            target_str = str(target_parquet.resolve())

            if ext in {".parquet", ".pq"}:
                read_expr = f"parquet_scan('{temp_path_str}')"
            elif ext in {".csv", ".tsv"}:
                read_expr = f"read_csv_auto('{temp_path_str}')"
            elif ext in {".json", ".jsonl"}:
                read_expr = f"read_json_auto('{temp_path_str}')"

            if mode == "append" and target_parquet.exists():
                sql = f"""
                    CREATE OR REPLACE TABLE _combined_upload AS
                    SELECT * FROM parquet_scan('{target_str}')
                    UNION ALL BY NAME
                    SELECT * FROM {read_expr};
                """
                self.conn.execute(sql)
                self.conn.execute(f"COPY _combined_upload TO '{target_str}' (FORMAT PARQUET, COMPRESSION ZSTD);")
                self.conn.execute("DROP TABLE IF EXISTS _combined_upload;")
            else:
                sql = f"COPY (SELECT * FROM {read_expr}) TO '{target_str}' (FORMAT PARQUET, COMPRESSION ZSTD);"
                self.conn.execute(sql)

            self.scan_and_sync()
            total_rows = self.conn.execute(f"SELECT count(*) FROM parquet_scan('{target_str}')").fetchone()[0]
            return {
                "status": "success",
                "table_name": sanitized_table,
                "project_id": proj_slug or "default",
                "mode": mode,
                "source_format": ext.lstrip("."),
                "total_rows": total_rows,
                "file_path": target_str,
                "file_size_bytes": target_parquet.stat().st_size
            }
        except Exception as e:
            logger.error(f"Failed to ingest file into DuckLake: {e}")
            return {"status": "error", "error": str(e)}
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def flush_inlined_data(self, table_name: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Flushes inlined small-record buffers from PostgreSQL catalog into persistent Parquet files (Chapter 3: CALL ducklake_flush_inlined_data)."""
        flushed_tables = 0
        total_flushed_rows = 0
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    if table_name and project_id:
                        cur.execute("SELECT DISTINCT table_name, project_id FROM ducklake_inlined_data WHERE table_name = %s AND project_id = %s;", (table_name, project_id))
                    elif table_name:
                        cur.execute("SELECT DISTINCT table_name, project_id FROM ducklake_inlined_data WHERE table_name = %s;", (table_name,))
                    elif project_id:
                        cur.execute("SELECT DISTINCT table_name, project_id FROM ducklake_inlined_data WHERE project_id = %s;", (project_id,))
                    else:
                        cur.execute("SELECT DISTINCT table_name, project_id FROM ducklake_inlined_data;")
                    targets = cur.fetchall()

                    for t_name, p_id in targets:
                        cur.execute("SELECT inlined_id, row_data FROM ducklake_inlined_data WHERE table_name = %s AND project_id = %s ORDER BY inlined_id;", (t_name, p_id))
                        rows = cur.fetchall()
                        if not rows:
                            continue
                        inlined_ids = [r[0] for r in rows]
                        records = [r[1] for r in rows]
                        
                        proj_slug = self.sanitize_table_name(p_id) if p_id and p_id != "default" else None
                        sanitized_t = self.sanitize_table_name(t_name)
                        if proj_slug:
                            t_dir = self.ducklake_dir / proj_slug
                            t_dir.mkdir(parents=True, exist_ok=True)
                            target_parquet = t_dir / f"{sanitized_t}.parquet"
                        else:
                            target_parquet = self.ducklake_dir / f"{sanitized_t}.parquet"
                        
                        df = pd.DataFrame(records)
                        self.conn.register("_flush_df", df)
                        p_str = str(target_parquet.resolve())
                        if target_parquet.exists():
                            self.conn.execute(f"""
                                CREATE OR REPLACE TABLE _combined_flush AS
                                SELECT * FROM parquet_scan('{p_str}')
                                UNION ALL BY NAME
                                SELECT * FROM _flush_df;
                            """)
                            self.conn.execute(f"COPY _combined_flush TO '{p_str}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);")
                            self.conn.execute("DROP TABLE IF EXISTS _combined_flush;")
                        else:
                            self.conn.execute(f"COPY (SELECT * FROM _flush_df) TO '{p_str}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);")
                        self.conn.unregister("_flush_df")

                        cur.execute("DELETE FROM ducklake_inlined_data WHERE inlined_id = ANY(%s);", (inlined_ids,))
                        conn.commit()
                        flushed_tables += 1
                        total_flushed_rows += len(records)
                        self.create_snapshot("FLUSH_INLINED", f"Flushed {len(records)} inlined rows into Parquet for table '{t_name}' in project '{p_id}'")

            self.scan_and_sync()
            return {"status": "success", "flushed_tables": flushed_tables, "flushed_rows": total_flushed_rows}
        except Exception as e:
            logger.error(f"Failed to flush inlined data: {e}")
            return {"status": "error", "error": str(e)}

    def merge_adjacent_files(self, table_name: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Merges adjacent Parquet files in table partitions into optimized Parquet storage (Compaction per Chapter 5: CALL ducklake_merge_adjacent_files)."""
        compacted = 0
        try:
            self.flush_inlined_data(table_name, project_id)
            if project_id and project_id != "default":
                search_dir = self.ducklake_dir / self.sanitize_table_name(project_id)
            else:
                search_dir = self.ducklake_dir

            if not search_dir.exists():
                return {"status": "success", "compacted_tables": 0, "message": "No files found to compact"}

            targets = list(search_dir.glob("*.parquet"))
            for pfile in targets:
                if pfile.name.endswith(".tmp.parquet"):
                    continue
                if table_name and pfile.stem != self.sanitize_table_name(table_name):
                    continue
                p_str = str(pfile.resolve())
                temp_target = pfile.with_suffix(".tmp.parquet")
                self.conn.execute(f"COPY (SELECT * FROM parquet_scan('{p_str}')) TO '{str(temp_target)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);")
                temp_target.replace(pfile)
                compacted += 1

            snap_id = self.create_snapshot("COMPACT", f"Compacted {compacted} Parquet files in DuckLake")
            self.scan_and_sync()
            return {"status": "success", "compacted_tables": compacted, "snapshot_id": snap_id}
        except Exception as e:
            logger.error(f"Failed to compact files: {e}")
            return {"status": "error", "error": str(e)}

    def cleanup_old_files(self) -> Dict[str, Any]:
        """Physically removes expired / orphaned files from storage (Chapter 5: CALL ducklake_cleanup_old_files)."""
        cleaned = 0
        try:
            for tmp in self.ducklake_dir.rglob("*.tmp.parquet"):
                try:
                    tmp.unlink()
                    cleaned += 1
                except Exception:
                    pass
            snap_id = self.create_snapshot("CLEANUP", f"Cleaned up {cleaned} obsolete/temporary files")
            return {"status": "success", "cleaned_files": cleaned, "snapshot_id": snap_id}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def save_warehouse_file(self, file_storage, filename: str, subfolder: str = "", overwrite: bool = True, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Saves an uploaded raw file to /srv/Warehouse."""
        sec_filename = secure_filename(filename)
        if not sec_filename:
            return {"status": "error", "error": "Invalid filename"}

        clean_subfolder = subfolder.strip().strip("/\\")
        if project_id and project_id != "default":
            clean_proj = self.sanitize_table_name(project_id)
            clean_subfolder = f"{clean_proj}/{clean_subfolder}".strip("/")

        target_dir = self.warehouse_dir / clean_subfolder if clean_subfolder else self.warehouse_dir

        safe_path = resolve_safe_path(self.warehouse_dir, str(Path(clean_subfolder) / sec_filename))
        if not safe_path:
            return {"status": "error", "error": "Path traversal detected or invalid destination"}

        if safe_path.exists() and not overwrite:
            return {"status": "error", "error": f"File '{sec_filename}' already exists and overwrite is false"}

        os.makedirs(safe_path.parent, exist_ok=True)
        file_storage.save(str(safe_path))

        self.scan_and_sync()
        return {
            "status": "success",
            "file_name": sec_filename,
            "project_id": project_id or (clean_subfolder.split("/")[0] if clean_subfolder else "default"),
            "relative_path": str(safe_path.relative_to(self.warehouse_dir)),
            "file_path": str(safe_path),
            "file_size_bytes": safe_path.stat().st_size,
            "format": safe_path.suffix.lower().lstrip(".")
        }

    def write_warehouse_text(self, filename: str, content: str, mode: str = "overwrite") -> Dict[str, Any]:
        """Writes or appends raw text / JSON / CSV content into /srv/Warehouse."""
        safe_path = resolve_safe_path(self.warehouse_dir, filename)
        if not safe_path:
            return {"status": "error", "error": "Path traversal detected or invalid filename"}

        os.makedirs(safe_path.parent, exist_ok=True)
        write_mode = "a" if mode == "append" else "w"
        with open(safe_path, write_mode, encoding="utf-8") as f:
            f.write(content)

        self.scan_and_sync()
        return {
            "status": "success",
            "file_name": safe_path.name,
            "relative_path": str(safe_path.relative_to(self.warehouse_dir)),
            "file_path": str(safe_path),
            "bytes_written": len(content.encode("utf-8")),
            "file_size_bytes": safe_path.stat().st_size,
            "mode": mode
        }

    def get_table_preview(self, table_name: str, limit: int = 50, offset: int = 0, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Fetches schema and preview rows for a DuckLake table."""
        sanitized_table = self.sanitize_table_name(table_name)
        target_parquet = self.ducklake_dir / f"{sanitized_table}.parquet"
        if not target_parquet.exists():
            try:
                with self.get_pg_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT source_path FROM ducklake_tables WHERE table_name = %s;", (sanitized_table,))
                        row = cur.fetchone()
                        if row and os.path.exists(row[0]):
                            target_parquet = Path(row[0])
            except Exception:
                pass

        if not target_parquet.exists():
            return {"status": "error", "error": f"Table '{table_name}' does not exist in DuckLake"}

        p_str = str(target_parquet.resolve())
        limit = min(max(1, limit), 500)
        offset = max(0, offset)

        try:
            col_sql = ", ".join(f'"{c}"' for c in columns) if columns else "*"
            schema_rows = self.conn.execute(f"DESCRIBE SELECT * FROM parquet_scan('{p_str}') LIMIT 0").fetchall()
            schema_info = [{"name": r[0], "type": str(r[1])} for r in schema_rows]

            total_rows = self.conn.execute(f"SELECT count(*) FROM parquet_scan('{p_str}')").fetchone()[0]

            preview_res = self.conn.execute(f"SELECT {col_sql} FROM parquet_scan('{p_str}') LIMIT {limit} OFFSET {offset}")
            preview_cols = [d[0] for d in preview_res.description]
            preview_types = [str(d[1]) for d in preview_res.description]

            rows = []
            for r in preview_res.fetchall():
                formatted = []
                for v in r:
                    if hasattr(v, "isoformat"):
                        formatted.append(v.isoformat())
                    elif isinstance(v, decimal.Decimal):
                        formatted.append(float(v))
                    elif isinstance(v, (bytes, bytearray)):
                        formatted.append(f"<blob {len(v)} bytes>")
                    else:
                        formatted.append(v)
                rows.append(formatted)

            return {
                "status": "success",
                "table_name": sanitized_table,
                "file_path": p_str,
                "file_size_bytes": target_parquet.stat().st_size,
                "total_rows": total_rows,
                "limit": limit,
                "offset": offset,
                "schema": schema_info,
                "columns": preview_cols,
                "types": preview_types,
                "rows": rows
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def read_warehouse_preview(self, filename: str, max_lines: int = 100) -> Dict[str, Any]:
        """Reads a preview or structured view of a raw warehouse file."""
        safe_path = resolve_safe_path(self.warehouse_dir, filename)
        if not safe_path or not safe_path.exists() or safe_path.is_dir():
            matches = list(self.warehouse_dir.rglob(filename))
            if matches and matches[0].is_file():
                safe_path = matches[0]
            else:
                return {"status": "error", "error": f"File '{filename}' not found in Warehouse"}

        ext = safe_path.suffix.lower()
        file_size = safe_path.stat().st_size
        max_lines = min(max(1, max_lines), 1000)

        text_exts = {
            ".csv", ".tsv", ".json", ".jsonl", ".txt", ".log", ".xml", ".yaml",
            ".yml", ".sql", ".md", ".mbox", ".env", ".conf", ".ini", ".sh",
            ".bash", ".cfg", ".toml", ".properties"
        }
        if ext in text_exts or ext == "":
            try:
                lines = []
                with open(safe_path, "r", encoding="utf-8", errors="replace") as f:
                    for _ in range(max_lines):
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line.rstrip("\r\n"))
                return {
                    "status": "success",
                    "file_name": safe_path.name,
                    "relative_path": str(safe_path.relative_to(self.warehouse_dir)),
                    "file_size_bytes": file_size,
                    "format": ext.lstrip(".") or "text",
                    "is_binary": False,
                    "lines_returned": len(lines),
                    "content_preview": lines
                }
            except Exception as e:
                return {"status": "error", "error": f"Failed to read text preview: {e}"}
        else:
            return {
                "status": "success",
                "file_name": safe_path.name,
                "relative_path": str(safe_path.relative_to(self.warehouse_dir)),
                "file_size_bytes": file_size,
                "format": ext.lstrip("."),
                "is_binary": True,
                "message": f"Binary file: download directly via /warehouse/files/{safe_path.relative_to(self.warehouse_dir)}"
            }

    def get_warehouse_categories(self) -> List[Dict[str, Any]]:
        """
        Aggregates raw warehouse files by category/folder, returning
        file counts, total size, formats, and relative directory paths.
        """
        categories = []
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            COALESCE(category, 'general') AS cat_name,
                            count(*) AS file_count,
                            COALESCE(sum(file_size_bytes), 0) AS total_bytes,
                            array_agg(DISTINCT file_format) FILTER (WHERE file_format IS NOT NULL) AS formats
                        FROM ducklake_raw_files
                        GROUP BY category
                        ORDER BY cat_name ASC;
                    """)
                    for r in cur.fetchall():
                        cat_name = r[0]
                        count = int(r[1] or 0)
                        total_bytes = int(r[2] or 0)
                        formats = [f for f in (r[3] or []) if f]
                        dir_path = "" if cat_name == "general" else cat_name
                        categories.append({
                            "category": cat_name,
                            "directory": f"/srv/Warehouse/{dir_path}".rstrip("/"),
                            "relative_directory": dir_path or ".",
                            "file_count": count,
                            "total_size_bytes": total_bytes,
                            "formats": formats
                        })
        except Exception as e:
            logger.error(f"Failed to fetch warehouse categories: {e}")
        return categories

    def search_warehouse_files(self, query: str = "", category: Optional[str] = None, format_filter: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        """
        Performs recursive search across all raw warehouse files by query, category, and format.
        """
        conditions = []
        params = []

        if query:
            clean_q = f"%{query.strip()}%"
            conditions.append("(file_name ILIKE %s OR relative_path ILIKE %s OR file_path ILIKE %s OR category ILIKE %s)")
            params.extend([clean_q, clean_q, clean_q, clean_q])

        if category and category.lower() != "all":
            conditions.append("category = %s")
            params.append(category.strip())

        if format_filter and format_filter.lower() != "all":
            clean_fmt = format_filter.strip().lstrip(".").lower()
            conditions.append("LOWER(file_format) = %s")
            params.append(clean_fmt)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        limit = min(max(1, limit), 1000)
        offset = max(0, offset)

        files = []
        total_count = 0
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    count_sql = f"SELECT count(*) FROM ducklake_raw_files {where_clause};"
                    cur.execute(count_sql, tuple(params))
                    total_count = cur.fetchone()[0]

                    sql = f"""
                        SELECT file_name, relative_path, category, project_id, file_format, file_size_bytes, status, schema_info, last_scanned
                        FROM ducklake_raw_files
                        {where_clause}
                        ORDER BY category ASC, relative_path ASC, file_name ASC
                        LIMIT %s OFFSET %s;
                    """
                    cur.execute(sql, tuple(params + [limit, offset]))
                    for r in cur.fetchall():
                        files.append({
                            "file_name": r[0],
                            "relative_path": r[1] or r[0],
                            "category": r[2] or "general",
                            "project_id": r[3] or "default",
                            "format": r[4] or "unknown",
                            "file_size_bytes": r[5] or 0,
                            "status": r[6] or "raw",
                            "schema": r[7] if isinstance(r[7], list) else (json.loads(r[7]) if r[7] else []),
                            "last_scanned": r[8].isoformat() if r[8] else None
                        })
        except Exception as e:
            logger.error(f"Failed to search warehouse files: {e}")
            return {"status": "error", "error": str(e), "files": [], "total_count": 0}

        return {
            "status": "success",
            "query": query,
            "category": category,
            "format": format_filter,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "files": files
        }

    def delete_table(self, table_name: str) -> Tuple[Dict[str, Any], int]:
        """Deletes a Parquet table and removes it from PostgreSQL metadata."""
        sanitized = self.sanitize_table_name(table_name)
        target = self.ducklake_dir / f"{sanitized}.parquet"
        if not target.exists():
            try:
                with self.get_pg_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT source_path FROM ducklake_tables WHERE table_name = %s;", (sanitized,))
                        row = cur.fetchone()
                        if row and os.path.exists(row[0]):
                            target = Path(row[0])
            except Exception:
                pass

        if not target.exists():
            return {"status": "error", "error": f"Table '{table_name}' does not exist in DuckLake"}, 404
        try:
            os.remove(target)
            self.conn.execute(f'DROP VIEW IF EXISTS "{sanitized}";')
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ducklake_tables WHERE table_name = %s;", (sanitized,))
                    conn.commit()
            logger.info(f"Deleted DuckLake table '{sanitized}' ({target})")
            return {"status": "success", "message": f"Table '{sanitized}' deleted"}, 200
        except Exception as e:
            logger.error(f"Failed to delete table {sanitized}: {e}")
            return {"status": "error", "error": str(e)}, 500

    def delete_warehouse_file(self, filename: str) -> Tuple[Dict[str, Any], int]:
        """Deletes a raw warehouse file and removes it from PostgreSQL metadata."""
        safe_path = resolve_safe_path(self.warehouse_dir, filename)
        if not safe_path or not safe_path.exists() or safe_path.is_dir():
            matches = list(self.warehouse_dir.rglob(filename))
            if matches and matches[0].is_file():
                safe_path = matches[0]
            else:
                return {"status": "error", "error": f"File '{filename}' not found in Warehouse"}, 404
        try:
            full_str = str(safe_path.resolve())
            rel_str = str(safe_path.relative_to(self.warehouse_dir))
            os.remove(safe_path)
            view_name = f"raw_{self.sanitize_table_name(safe_path.stem)}"
            self.conn.execute(f'DROP VIEW IF EXISTS "{view_name}";')
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ducklake_raw_files WHERE file_path = %s OR relative_path = %s;", (full_str, rel_str))
                    conn.commit()
            logger.info(f"Deleted warehouse file '{filename}' ({safe_path})")
            return {"status": "success", "message": f"File '{filename}' deleted"}, 200
        except Exception as e:
            logger.error(f"Failed to delete warehouse file {filename}: {e}")
            return {"status": "error", "error": str(e)}, 500

    def list_projects(self) -> List[Dict[str, Any]]:
        """Lists all registered projects with table counts, raw file counts, and disk usage."""
        projects = []
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            p.project_id, 
                            p.display_name, 
                            p.description, 
                            p.created_at,
                            COALESCE(t.table_count, 0) AS table_count,
                            COALESCE(t.total_rows, 0) AS total_rows,
                            COALESCE(t.total_parquet_bytes, 0) AS total_parquet_bytes,
                            COALESCE(w.file_count, 0) AS raw_files_count,
                            COALESCE(w.total_warehouse_bytes, 0) AS total_warehouse_bytes
                        FROM ducklake_projects p
                        LEFT JOIN (
                            SELECT project_id, count(*) AS table_count, sum(row_count) AS total_rows, sum(file_size_bytes) AS total_parquet_bytes
                            FROM ducklake_tables GROUP BY project_id
                        ) t ON p.project_id = t.project_id
                        LEFT JOIN (
                            SELECT project_id, count(*) AS file_count, sum(file_size_bytes) AS total_warehouse_bytes
                            FROM ducklake_raw_files GROUP BY project_id
                        ) w ON p.project_id = w.project_id
                        ORDER BY p.project_id ASC;
                    """)
                    for r in cur.fetchall():
                        pid = r[0]
                        parquet_b = int(r[6] or 0)
                        wh_b = int(r[8] or 0)
                        projects.append({
                            "project_id": pid,
                            "display_name": r[1] or pid,
                            "description": r[2] or "",
                            "created_at": r[3].isoformat() if r[3] else None,
                            "tables_count": int(r[4] or 0),
                            "total_rows": int(r[5] or 0),
                            "parquet_bytes": parquet_b,
                            "raw_files_count": int(r[7] or 0),
                            "warehouse_bytes": wh_b,
                            "total_bytes": parquet_b + wh_b,
                            "lake_dir": str(self.ducklake_dir / (pid if pid != "default" else "")),
                            "warehouse_dir": str(self.warehouse_dir / (pid if pid != "default" else ""))
                        })
        except Exception as e:
            logger.error(f"Failed to list projects: {e}")
        return projects

    def create_project(self, project_id: str, display_name: Optional[str] = None, description: str = "") -> Dict[str, Any]:
        """Creates a new project partition in DuckLake and Warehouse."""
        sanitized_id = self.sanitize_table_name(project_id)
        if not sanitized_id:
            return {"status": "error", "error": "Invalid project_id"}
        
        display = display_name.strip() if display_name else sanitized_id

        if sanitized_id != "default":
            proj_lake = self.ducklake_dir / sanitized_id
            proj_wh = self.warehouse_dir / sanitized_id
            os.makedirs(proj_lake, exist_ok=True)
            os.makedirs(proj_wh, exist_ok=True)

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO ducklake_projects (project_id, display_name, description, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (project_id) DO UPDATE SET
                            display_name = EXCLUDED.display_name,
                            description = EXCLUDED.description,
                            updated_at = CURRENT_TIMESTAMP;
                    """, (sanitized_id, display, description))
                    conn.commit()

            return {
                "status": "success",
                "project_id": sanitized_id,
                "display_name": display,
                "description": description,
                "lake_dir": str(self.ducklake_dir / (sanitized_id if sanitized_id != "default" else "")),
                "warehouse_dir": str(self.warehouse_dir / (sanitized_id if sanitized_id != "default" else "")),
                "archive_endpoints": {
                    "upload_table": f"/projects/{sanitized_id}/tables/<table_name>",
                    "upload_raw": f"/projects/{sanitized_id}/files/<filename>",
                    "append_records": f"/projects/{sanitized_id}/tables/<table_name>"
                }
            }
        except Exception as e:
            logger.error(f"Failed to create project {sanitized_id}: {e}")
            return {"status": "error", "error": str(e)}

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves project details, its tables, raw files, and curl archiving commands."""
        sanitized_id = self.sanitize_table_name(project_id)
        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT project_id, display_name, description, created_at, updated_at
                        FROM ducklake_projects WHERE project_id = %s;
                    """, (sanitized_id,))
                    p_row = cur.fetchone()
                    if not p_row:
                        return None
                    
                    cur.execute("""
                        SELECT table_name, format, row_count, file_size_bytes, source_path, updated_at
                        FROM ducklake_tables WHERE project_id = %s ORDER BY table_name ASC;
                    """, (sanitized_id,))
                    tables = [{
                        "table_name": r[0],
                        "format": r[1],
                        "row_count": r[2],
                        "file_size_bytes": r[3],
                        "source_path": r[4],
                        "updated_at": r[5].isoformat() if r[5] else None
                    } for r in cur.fetchall()]

                    cur.execute("""
                        SELECT file_name, file_format, file_size_bytes, file_path, last_scanned, relative_path, category
                        FROM ducklake_raw_files WHERE project_id = %s ORDER BY relative_path ASC, file_name ASC;
                    """, (sanitized_id,))
                    raw_files = [{
                        "file_name": r[0],
                        "file_format": r[1],
                        "file_size_bytes": r[2],
                        "file_path": r[3],
                        "last_scanned": r[4].isoformat() if r[4] else None,
                        "relative_path": r[5] or r[0],
                        "category": r[6] or "general"
                    } for r in cur.fetchall()]

                    return {
                        "project_id": p_row[0],
                        "display_name": p_row[1],
                        "description": p_row[2],
                        "created_at": p_row[3].isoformat() if p_row[3] else None,
                        "updated_at": p_row[4].isoformat() if p_row[4] else None,
                        "tables": tables,
                        "raw_files": raw_files,
                        "stats": {
                            "tables_count": len(tables),
                            "raw_files_count": len(raw_files),
                            "total_rows": sum(t["row_count"] or 0 for t in tables),
                            "total_bytes": sum(t["file_size_bytes"] or 0 for t in tables) + sum(f["file_size_bytes"] or 0 for f in raw_files)
                        },
                        "archive_snippets": {
                            "upload_parquet_table": f"curl -X PUT http://<server-ip>:5000/projects/{sanitized_id}/tables/<table_name> -F 'file=@data.csv'",
                            "upload_raw_file": f"curl -X PUT http://<server-ip>:5000/projects/{sanitized_id}/files/<filename> -F 'file=@archive.tar.gz'",
                            "append_json": f"curl -X PATCH http://<server-ip>:5000/projects/{sanitized_id}/tables/<table_name> -H 'Content-Type: application/json' -d '[{{\"id\": 1, \"status\": \"ok\"}}]'"
                        }
                    }
        except Exception as e:
            logger.error(f"Failed to get project {sanitized_id}: {e}")
            return None

    def delete_project(self, project_id: str, purge_files: bool = False) -> Tuple[Dict[str, Any], int]:
        """Deletes a project partition and optionally purges its files from disk."""
        sanitized_id = self.sanitize_table_name(project_id)
        if sanitized_id == "default":
            return {"status": "error", "error": "The 'default' project cannot be deleted."}, 400

        try:
            with self.get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ducklake_projects WHERE project_id = %s;", (sanitized_id,))
                    if purge_files:
                        cur.execute("DELETE FROM ducklake_tables WHERE project_id = %s;", (sanitized_id,))
                        cur.execute("DELETE FROM ducklake_raw_files WHERE project_id = %s;", (sanitized_id,))
                    else:
                        cur.execute("UPDATE ducklake_tables SET project_id = 'default' WHERE project_id = %s;", (sanitized_id,))
                        cur.execute("UPDATE ducklake_raw_files SET project_id = 'default' WHERE project_id = %s;", (sanitized_id,))
                    conn.commit()

            if purge_files:
                proj_lake = self.ducklake_dir / sanitized_id
                proj_wh = self.warehouse_dir / sanitized_id
                if proj_lake.exists() and proj_lake.is_dir():
                    shutil.rmtree(proj_lake, ignore_errors=True)
                if proj_wh.exists() and proj_wh.is_dir():
                    shutil.rmtree(proj_wh, ignore_errors=True)
                self.scan_and_sync()

            return {"status": "success", "message": f"Project '{sanitized_id}' deleted", "purged": purge_files}, 200
        except Exception as e:
            logger.error(f"Failed to delete project {sanitized_id}: {e}")
            return {"status": "error", "error": str(e)}, 500
