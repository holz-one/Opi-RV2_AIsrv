#!/usr/bin/env python3
"""
aisrv.py - DuckLake Analytical Server & Local AI Inference Engine for Orange Pi RV2
===================================================================================
Features:
  - DuckLake Architecture:
      * DuckDB in-memory / attached analytical query engine.
      * PostgreSQL as central metadata catalog (dataset schemas, table registry,
        warehouse inventory, model registry, chat logs, query history).
      * Parquet files hosted in /srv/DuckLake.
      * Raw data files hosted in /srv/Warehouse (CSV, TSV, JSON, SQLite, Excel, etc.).
  - AI Inference:
      * ONNX Runtime GenAI local LLM inference engine.
      * OpenAI-compatible /v1/models and /v1/chat/completions (streaming & standard).
  - Network Security:
      * AI inference endpoints gated strictly to 10.0.2.0/24 (br0 wired network).
      * DuckLake analytical services and console accessible to all clients.
  - Configuration:
      * server.yaml for all deployment parameters.
      * .env for credentials and secrets.
"""

import argparse
import decimal
import io
import ipaddress
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from flask import Flask, Response, jsonify, render_template, request, send_file, send_from_directory, stream_with_context
import pandas as pd
import psycopg
from werkzeug.utils import secure_filename
import yaml

# Optional ONNX Runtime GenAI import (graceful fallback if weights not loaded)
try:
    import onnxruntime_genai as og
    OG_AVAILABLE = True
except ImportError:
    og = None
    OG_AVAILABLE = False

# ==============================================================================
# Logging Configuration
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DuckLake-AI")

# ==============================================================================
# Configuration & Environment Management
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent

def load_dotenv_file(filepath: Optional[Path] = None):
    """Loads key-value pairs from .env into os.environ without overriding existing env."""
    if filepath is None:
        filepath = BASE_DIR / ".env"
    if not filepath.exists():
        return
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v
        logger.info(f"Loaded environment variables from {filepath}")
    except Exception as e:
        logger.warning(f"Failed to read .env file at {filepath}: {e}")

# Load .env first
load_dotenv_file()

def load_server_config(filepath: Optional[Path] = None) -> Dict[str, Any]:
    """Loads server.yaml and merges environment variable overrides."""
    if filepath is None:
        filepath = BASE_DIR / "server.yaml"
    
    cfg: Dict[str, Any] = {
        "server": {
            "host": "0.0.0.0",
            "port": 5000,
            "debug": False,
            "cors_enabled": True,
        },
        "postgres": {
            "host": "",
            "port": 5432,
            "dbname": "orangepi",
            "user": "orangepi",
            "password": "",
            "connect_timeout": 10,
        },
        "storage": {
            "ducklake_dir": "/srv/DuckLake",
            "warehouse_dir": "/srv/Warehouse",
            "models_dir": "/cluster/RV2-AI",
            "db_cache_dir": "/cluster/database",
            "max_upload_size_mb": 250,
        },
        "ducklake": {
            "memory_limit": "1GB",
            "threads": 4,
            "max_query_rows": 5000,
            "auto_scan_on_startup": True,
            "auto_create_views": True,
        },
        "ai": {
            "enabled": True,
            "default_context_length": 2048,
            "default_max_tokens": 256,
            "default_temperature": 0.7,
            "repetition_penalty": 1.1,
        },
        "security": {
            "ai_allowed_cidrs": ["10.0.2.0/24", "127.0.0.1/32", "::1/128"],
            "trust_forwarded_for": True,
        },
    }

    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    for section, values in loaded.items():
                        if isinstance(values, dict) and section in cfg:
                            cfg[section].update(values)
                        else:
                            cfg[section] = values
            logger.info(f"Loaded configuration from {filepath}")
        except Exception as e:
            logger.error(f"Error parsing {filepath}, falling back to defaults: {e}")

    # Environment variable overrides
    if os.getenv("SERVER_HOST"):
        cfg["server"]["host"] = os.getenv("SERVER_HOST")
    if os.getenv("SERVER_PORT"):
        cfg["server"]["port"] = int(os.getenv("SERVER_PORT"))
    if os.getenv("PG_HOST"):
        cfg["postgres"]["host"] = os.getenv("PG_HOST")
    if os.getenv("PG_PORT"):
        cfg["postgres"]["port"] = int(os.getenv("PG_PORT"))
    if os.getenv("PG_DBNAME"):
        cfg["postgres"]["dbname"] = os.getenv("PG_DBNAME")
    if os.getenv("PG_USER"):
        cfg["postgres"]["user"] = os.getenv("PG_USER")
    if os.getenv("PG_PASSWORD") is not None:
        cfg["postgres"]["password"] = os.getenv("PG_PASSWORD")
    if os.getenv("DUCKLAKE_MEMORY_LIMIT"):
        cfg["ducklake"]["memory_limit"] = os.getenv("DUCKLAKE_MEMORY_LIMIT")
    if os.getenv("DUCKLAKE_THREADS"):
        cfg["ducklake"]["threads"] = int(os.getenv("DUCKLAKE_THREADS"))

    # Cleanup legacy symlink /srv/Wearhouse if detected
    if os.path.islink("/srv/Wearhouse"):
        try:
            os.unlink("/srv/Wearhouse")
        except Exception:
            pass

    return cfg

def save_server_config(updates: Dict[str, Any], filepath: Optional[Path] = None) -> Dict[str, Any]:
    """Updates server.yaml file and reloads in-memory CONFIG."""
    global CONFIG
    if filepath is None:
        filepath = Path(__file__).resolve().parent / "server.yaml"

    current = {}
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                current = yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to read existing config: {e}")

    for section, values in updates.items():
        if isinstance(values, dict) and section in current and isinstance(current[section], dict):
            current[section].update(values)
        else:
            current[section] = values

    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(current, f, default_flow_style=False, sort_keys=False)

    CONFIG = load_server_config(filepath)
    return CONFIG

CONFIG = load_server_config()

# Ensure directories exist
os.makedirs(CONFIG["storage"]["ducklake_dir"], exist_ok=True)
os.makedirs(CONFIG["storage"]["warehouse_dir"], exist_ok=True)
os.makedirs(CONFIG["storage"]["db_cache_dir"], exist_ok=True)

# ==============================================================================
# PostgreSQL Metadata Layer
# ==============================================================================
def get_pg_connection():
    """Establishes connection to PostgreSQL metadata database."""
    pg = CONFIG["postgres"]
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

def init_postgres_metadata():
    """Initializes metadata tables and views in PostgreSQL."""
    try:
        with get_pg_connection() as conn:
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
                """)

                # Seed default project partition
                cur.execute("""
                    INSERT INTO ducklake_projects (project_id, display_name, description)
                    VALUES ('default', 'Default Lake', 'Root partition for standalone tables and general raw warehouse files')
                    ON CONFLICT (project_id) DO NOTHING;
                """)

                # Seed DawahTools partition if existing
                dawah_dir = Path(CONFIG["storage"]["ducklake_dir"]) / "DawahTools"
                if dawah_dir.exists():
                    cur.execute("""
                        INSERT INTO ducklake_projects (project_id, display_name, description)
                        VALUES ('dawahtools', 'DawahTools', 'Comparative religion datasets, Bible and Hadith collections')
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

                # 6. Backward Compatibility Views (for legacy code/tools querying models/chat_logs)
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
                logger.info("PostgreSQL DuckLake & AI metadata schemas verified successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL metadata: {e}")

def migrate_legacy_duckdb_models():
    """One-time migration of existing models from models.duckdb to PostgreSQL if empty."""
    legacy_duckdb_path = Path(CONFIG["storage"]["db_cache_dir"]) / "models.duckdb"
    if not legacy_duckdb_path.exists():
        return

    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM ai_models;")
                count = cur.fetchone()[0]
                if count == 0:
                    logger.info(f"Importing legacy models from {legacy_duckdb_path} to PostgreSQL...")
                    d_conn = duckdb.connect(str(legacy_duckdb_path), read_only=True)
                    rows = d_conn.execute("SELECT id, owned_by, directory_path, variant_name FROM models").fetchall()
                    d_conn.close()
                    for r in rows:
                        cur.execute("""
                            INSERT INTO ai_models (id, owned_by, directory_path, variant_name, updated_at)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (id) DO UPDATE SET directory_path = EXCLUDED.directory_path;
                        """, r)
                    conn.commit()
                    logger.info(f"Successfully migrated {len(rows)} models into PostgreSQL ai_models.")
    except Exception as e:
        logger.warning(f"Could not migrate legacy models from {legacy_duckdb_path}: {e}")

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

# ==============================================================================
# DuckLake Query Engine (DuckDB In-Memory Analytical Processor)
# ==============================================================================
class DuckLakeEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ducklake_dir = Path(config["storage"]["ducklake_dir"]).resolve()
        self.warehouse_dir = Path(config["storage"]["warehouse_dir"]).resolve()
        self.conn = duckdb.connect(database=":memory:")
        self._apply_pragmas()
        if self.config["ducklake"].get("auto_scan_on_startup", True):
            try:
                self.scan_and_sync()
            except Exception as e:
                logger.warning(f"Initial DuckLake storage scan failed: {e}")

    def _apply_pragmas(self):
        mem_limit = self.config["ducklake"].get("memory_limit", "1GB")
        threads = self.config["ducklake"].get("threads", 4)
        try:
            self.conn.execute(f"PRAGMA memory_limit='{mem_limit}';")
            self.conn.execute(f"PRAGMA threads={threads};")
            logger.info(f"DuckLake Engine initialized (memory_limit={mem_limit}, threads={threads})")
        except Exception as e:
            logger.warning(f"Failed to set DuckDB pragmas: {e}")

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
            with get_pg_connection() as conn:
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
            with get_pg_connection() as conn:
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
            with get_pg_connection() as conn:
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

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """Retrieves DuckLake catalog snapshot transaction history (Chapter 4: FROM snapshots())."""
        try:
            with get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT s.snapshot_id, s.snapshot_time, s.schema_version, 
                               COALESCE(sc.operation, 'SNAPSHOT') AS operation, 
                               COALESCE(sc.description, '') AS description
                        FROM ducklake_snapshot s
                        LEFT JOIN ducklake_snapshot_changes sc ON s.snapshot_id = sc.snapshot_id
                        ORDER BY s.snapshot_id DESC LIMIT 100;
                    """)
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
                with get_pg_connection() as pg_conn:
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
                if self.config["ducklake"].get("auto_create_views", True):
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

        # 2. Scan Raw Warehouse Files
        supported_raw_exts = {".csv", ".tsv", ".json", ".jsonl", ".parquet", ".sqlite", ".db", ".xlsx", ".ods"}
        for root, _, files in os.walk(str(self.warehouse_dir)):
            for f in files:
                f_path = Path(root) / f
                ext = f_path.suffix.lower()
                if ext in supported_raw_exts:
                    try:
                        f_str = str(f_path.resolve())
                        f_size = f_path.stat().st_size
                        f_format = ext.lstrip(".")
                        schema_info = []
                        row_count = None

                        rel_wh_parts = f_path.relative_to(self.warehouse_dir).parts
                        if len(rel_wh_parts) > 1:
                            proj_id = self.sanitize_table_name(rel_wh_parts[0])
                            proj_name = rel_wh_parts[0]
                        else:
                            proj_id = "default"
                            proj_name = "Default Lake"

                        # Sample CSV / JSON schemas via DuckDB where possible
                        if ext in {".csv", ".tsv"}:
                            try:
                                d_schema = self.conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{f_str}') LIMIT 0").fetchall()
                                schema_info = [{"name": r[0], "type": r[1]} for r in d_schema]
                                # Create raw view in DuckDB
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

                        with get_pg_connection() as pg_conn:
                            with pg_conn.cursor() as cur:
                                if proj_id != "default":
                                    cur.execute("""
                                        INSERT INTO ducklake_projects (project_id, display_name, description)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (project_id) DO NOTHING;
                                    """, (proj_id, proj_name, f"Project partition for {proj_name}"))

                                cur.execute("""
                                    INSERT INTO ducklake_raw_files (file_path, project_id, file_name, file_format, file_size_bytes, schema_info, last_scanned)
                                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                    ON CONFLICT (file_path) DO UPDATE SET
                                        project_id = EXCLUDED.project_id,
                                        file_size_bytes = EXCLUDED.file_size_bytes,
                                        schema_info = EXCLUDED.schema_info,
                                        last_scanned = CURRENT_TIMESTAMP;
                                """, (f_str, proj_id, f, f_format, f_size, json.dumps(schema_info)))
                                pg_conn.commit()

                        stats["warehouse_files"] += 1
                    except Exception as e:
                        err_msg = f"Failed to catalog raw file {f_path}: {e}"
                        logger.error(err_msg)
                        stats["errors"].append(err_msg)

        # 3. Expose Canonical DuckLake Metadata Views in DuckDB (Chapter 4: FROM snapshots())
        try:
            with get_pg_connection() as pg_conn:
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

                    # Inlined-only tables (tables that have inlined records but no Parquet file yet)
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

    def execute_query(self, sql_query: str, client_ip: str) -> Dict[str, Any]:
        """Executes analytical SQL query on DuckDB and records telemetry in PostgreSQL."""
        start_time = time.time()
        max_rows = self.config["ducklake"].get("max_query_rows", 5000)
        
        try:
            rel = self.conn.execute(sql_query)
            cols = [desc[0] for desc in rel.description] if rel.description else []
            col_types = [str(desc[1]) for desc in rel.description] if rel.description else []
            
            raw_rows = rel.fetchmany(max_rows)
            # Format row values for JSON serialization
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

            # Log to PostgreSQL
            try:
                with get_pg_connection() as pg_conn:
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
                with get_pg_connection() as pg_conn:
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
        source_path = self.warehouse_dir / raw_filename
        if not source_path.exists():
            # Check recursive match
            matches = list(self.warehouse_dir.rglob(raw_filename))
            if matches:
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
            # Update raw file status in PostgreSQL
            with get_pg_connection() as pg_conn:
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
                with get_pg_connection() as conn:
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

            # Retrieve prior inlined records if any to merge into Parquet
            prior_inlined = []
            with get_pg_connection() as conn:
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
                with get_pg_connection() as conn:
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
            with get_pg_connection() as conn:
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
            # Check PostgreSQL ducklake_tables for full source_path (handles nested project tables)
            try:
                with get_pg_connection() as conn:
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
        if not safe_path or not safe_path.exists():
            return {"status": "error", "error": f"File '{filename}' not found in Warehouse"}

        ext = safe_path.suffix.lower()
        file_size = safe_path.stat().st_size
        max_lines = min(max(1, max_lines), 1000)

        text_exts = {".csv", ".tsv", ".json", ".jsonl", ".txt", ".log", ".xml", ".yaml", ".yml", ".sql", ".md"}
        if ext in text_exts:
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
                    "format": ext.lstrip("."),
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
                "message": "Binary file: download directly via /warehouse/files/{filename}"
            }

    def delete_table(self, table_name: str) -> Tuple[Dict[str, Any], int]:
        """Deletes a Parquet table and removes it from PostgreSQL metadata."""
        sanitized = self.sanitize_table_name(table_name)
        target = self.ducklake_dir / f"{sanitized}.parquet"
        if not target.exists():
            try:
                with get_pg_connection() as conn:
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
            with get_pg_connection() as conn:
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
            return {"status": "error", "error": f"File '{filename}' not found in Warehouse"}, 404
        try:
            os.remove(safe_path)
            view_name = f"raw_{self.sanitize_table_name(safe_path.stem)}"
            self.conn.execute(f'DROP VIEW IF EXISTS "{view_name}";')
            with get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ducklake_raw_files WHERE file_path = %s;", (str(safe_path),))
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
            with get_pg_connection() as conn:
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

        # Ensure directories
        if sanitized_id != "default":
            proj_lake = self.ducklake_dir / sanitized_id
            proj_wh = self.warehouse_dir / sanitized_id
            os.makedirs(proj_lake, exist_ok=True)
            os.makedirs(proj_wh, exist_ok=True)

        try:
            with get_pg_connection() as conn:
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
            with get_pg_connection() as conn:
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
                        SELECT file_name, file_format, file_size_bytes, file_path, last_scanned
                        FROM ducklake_raw_files WHERE project_id = %s ORDER BY file_name ASC;
                    """, (sanitized_id,))
                    raw_files = [{
                        "file_name": r[0],
                        "file_format": r[1],
                        "file_size_bytes": r[2],
                        "file_path": r[3],
                        "last_scanned": r[4].isoformat() if r[4] else None
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
            with get_pg_connection() as conn:
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

# ==============================================================================
# Security & Network Access Control
# ==============================================================================
def get_client_ip() -> str:
    """Extracts client IP, respecting X-Forwarded-For if configured."""
    if CONFIG["security"].get("trust_forwarded_for", True):
        x_forwarded = request.headers.get("X-Forwarded-For")
        if x_forwarded:
            return x_forwarded.split(",")[0].strip()
        x_real_ip = request.headers.get("X-Real-IP")
        if x_real_ip:
            return x_real_ip.strip()
    return request.remote_addr or "127.0.0.1"

def is_ip_allowed_for_ai(client_ip: str) -> bool:
    """
    Checks if client IP is within permitted networks for AI features (default 10.0.2.0/24 + loopback).
    DuckLake features are accessible cluster-wide.
    """
    allowed_cidrs = CONFIG["security"].get("ai_allowed_cidrs", ["10.0.2.0/24", "127.0.0.1/32", "::1/128"])
    try:
        ip = ipaddress.ip_address(client_ip)
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        for cidr in allowed_cidrs:
            net = ipaddress.ip_network(cidr, strict=False)
            if ip in net:
                return True
    except Exception as e:
        logger.warning(f"Error checking IP permission for {client_ip}: {e}")
    return False

def require_ai_network(f):
    """Decorator to enforce that AI features are only accessible from 10.0.2.0/24 (and localhost)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_ip = get_client_ip()
        if not is_ip_allowed_for_ai(client_ip):
            allowed_cidrs = CONFIG["security"].get("ai_allowed_cidrs", ["10.0.2.0/24"])
            logger.warning(f"Forbidden AI access attempt from {client_ip}. Required networks: {allowed_cidrs}")
            return jsonify({
                "error": {
                    "message": f"Access denied: AI features are restricted to the {allowed_cidrs[0]} network.",
                    "type": "network_access_denied",
                    "client_ip": client_ip,
                    "allowed_networks": allowed_cidrs
                }
            }), 403
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# AI Model Registry & Inference (onnxruntime-genai)
# ==============================================================================
loaded_models: Dict[str, Any] = {}

def sync_models_to_db():
    """Scans storage.models_dir for ONNX models and registers metadata in PostgreSQL."""
    base_dir = CONFIG["storage"]["models_dir"]
    if not os.path.exists(base_dir):
        return

    for root, dirs, files in os.walk(base_dir):
        if ".cache" in root or ".git" in root:
            continue

        has_genai = "genai_config.json" in files
        has_standard = "config.json" in files

        if has_genai or has_standard:
            model_dir = root
            config_path = os.path.join(model_dir, "genai_config.json")

            # Auto-generate minimal genai_config.json if only standard config.json is present
            if not has_genai and has_standard:
                try:
                    onnx_files = [f for f in files if f.endswith(".onnx")]
                    if onnx_files or "onnx" in dirs:
                        basic_config = {
                            "model_type": "llama",
                            "vocab_size": 32000,
                            "hidden_size": 2048,
                            "num_attention_heads": 32,
                            "num_hidden_layers": 22,
                            "context_length": CONFIG["ai"].get("default_context_length", 2048),
                            "provider": "cpu",
                        }
                        try:
                            with open(os.path.join(model_dir, "config.json"), "r") as cf:
                                c_data = json.load(cf)
                                if "model_type" in c_data:
                                    basic_config["model_type"] = c_data["model_type"]
                        except Exception:
                            pass

                        with open(config_path, "w") as gf:
                            json.dump(basic_config, gf, indent=2)
                except Exception as e:
                    logger.debug(f"Skipping config generation for {model_dir}: {e}")

            if os.path.exists(config_path):
                rel_path = os.path.relpath(model_dir, base_dir)
                clean_id = rel_path.replace(os.sep, "/")
                variant_name = os.path.basename(model_dir)

                with get_pg_connection() as pg_conn:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO ai_models (id, owned_by, directory_path, variant_name, updated_at)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (id) DO UPDATE SET
                                directory_path = EXCLUDED.directory_path,
                                variant_name = EXCLUDED.variant_name,
                                updated_at = CURRENT_TIMESTAMP;
                        """, (clean_id, "local", model_dir, variant_name))
                        pg_conn.commit()

def get_detailed_models_list() -> List[Dict[str, Any]]:
    """Retrieves full rich model inventory for UI and management endpoints."""
    sync_models_to_db()
    models = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, owned_by, directory_path, variant_name, model_type, created_at, updated_at
                    FROM ai_models ORDER BY id ASC;
                """)
                for r in cur.fetchall():
                    m_id, owned, dir_path, variant, m_type, created, updated = r
                    disk_size_mb = 0
                    p = Path(dir_path) if dir_path else None
                    if p and p.exists():
                        try:
                            total_bytes = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
                            disk_size_mb = round(total_bytes / (1024 * 1024), 1)
                        except Exception:
                            pass
                    models.append({
                        "id": m_id,
                        "owned_by": owned or "local",
                        "directory_path": dir_path,
                        "variant_name": variant or (Path(dir_path).name if dir_path else m_id),
                        "model_type": m_type or "genai_onnx",
                        "disk_size_mb": disk_size_mb,
                        "status": "ready" if (p and p.exists()) else "missing_files",
                        "context_length": CONFIG["ai"].get("default_context_length", 2048),
                        "default_max_tokens": CONFIG["ai"].get("default_max_tokens", 256),
                        "created_at": created.isoformat() if created else None
                    })
    except Exception as e:
        logger.error(f"Failed to get detailed models list: {e}")
    return models

def get_model_path(model_id: str) -> Optional[str]:
    """Resolves model id or alias to directory containing genai_config.json via PostgreSQL."""
    sync_models_to_db()
    with get_pg_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT directory_path FROM ai_models
                WHERE id = %s OR variant_name = %s OR id LIKE '%%/' || %s
                ORDER BY CASE WHEN id = %s THEN 1 ELSE 2 END
                LIMIT 1;
            """, (model_id, model_id, model_id, model_id))
            row = cur.fetchone()
            if row and os.path.exists(os.path.join(row[0], "genai_config.json")):
                return row[0]
    return None

def load_model_instance(model_id: str) -> Optional[Dict[str, Any]]:
    """Loads ONNX GenAI model and tokenizer into memory."""
    if not OG_AVAILABLE:
        logger.error("onnxruntime_genai is not installed or unavailable on this system.")
        return None

    if model_id in loaded_models:
        return loaded_models[model_id]

    path = get_model_path(model_id)
    if not path:
        logger.error(f"Model path not found for: {model_id}")
        return None

    logger.info(f"Loading ONNX model into memory: {model_id} from {path}...")
    try:
        model = og.Model(path)
        tokenizer = og.Tokenizer(model)
        loaded_models[model_id] = {"model": model, "tokenizer": tokenizer}
        return loaded_models[model_id]
    except Exception as e:
        logger.error(f"Failed to load ONNX model {model_id}: {e}")
        return None

def log_chat_interaction(model_id: str, role: str, content: str, client_ip: str, p_tokens: int = 0, c_tokens: int = 0):
    """Logs conversation interaction into PostgreSQL."""
    try:
        with get_pg_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_chat_logs (model_id, role, content, client_ip, prompt_tokens, completion_tokens)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (model_id, role, content, client_ip, p_tokens, c_tokens))
                pg_conn.commit()
    except Exception as e:
        logger.warning(f"Failed to log chat interaction: {e}")

# ==============================================================================
# Instantiate Engine & Initialize Metadata Catalog
# ==============================================================================
try:
    init_postgres_metadata()
    migrate_legacy_duckdb_models()
    sync_models_to_db()
except Exception as _e:
    logger.warning(f"Deferred PostgreSQL initialization: {_e}")

ducklake_engine = DuckLakeEngine(CONFIG)
if CONFIG["ducklake"].get("auto_scan_on_startup", True):
    try:
        ducklake_engine.scan_and_sync()
    except Exception as _e:
        logger.warning(f"Initial storage scan error: {_e}")

# ==============================================================================
# Service Management (Adminer & AI Chat Memory Toggles)
# ==============================================================================
def get_service_status(service_name: str) -> Dict[str, Any]:
    """Checks status and memory usage of background services (systemctl for Adminer, in-memory for AI)."""
    if service_name == "ai":
        enabled = CONFIG["ai"].get("enabled", True)
        loaded_names = list(loaded_models.keys())
        return {
            "service": "ai",
            "status": "active" if enabled else "stopped",
            "is_active": enabled,
            "memory_mb": 0.0,
            "loaded_models": loaded_names,
            "loaded_models_count": len(loaded_names),
            "onnx_available": OG_AVAILABLE
        }

    try:
        res = subprocess.run(
            ["systemctl", "is-active", service_name],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        is_active = res.stdout.strip() == "active"
        
        mem_mb = 0.0
        if is_active:
            mem_res = subprocess.run(
                ["systemctl", "show", service_name, "--property=MemoryCurrent"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            for line in mem_res.stdout.splitlines():
                if line.startswith("MemoryCurrent="):
                    val = line.split("=")[1].strip()
                    if val.isdigit() and 0 < int(val) < 18446744073709551615:
                        mem_mb = round(int(val) / (1024 * 1024), 1)

        return {
            "service": service_name,
            "status": "running" if is_active else "stopped",
            "is_active": is_active,
            "memory_mb": mem_mb,
            "port": 8080 if service_name == "adminer" else None
        }
    except Exception as e:
        return {"service": service_name, "status": "unknown", "error": str(e)}

def set_service_state(service_name: str, action: str) -> Tuple[Dict[str, Any], int]:
    """Starts, stops, or restarts a service (systemctl for Adminer, engine toggle & RAM unload for AI)."""
    valid_actions = {"start", "stop", "restart", "toggle", "enable", "disable", "on", "off"}
    if action not in valid_actions:
        return {"status": "error", "error": f"Invalid action '{action}'. Valid: {list(valid_actions)}"}, 400

    if service_name == "ai":
        curr_active = CONFIG["ai"].get("enabled", True)
        if action == "toggle":
            new_state = not curr_active
        elif action in {"start", "restart", "enable", "on"}:
            new_state = True
        else:
            new_state = False

        CONFIG["ai"]["enabled"] = new_state
        unloaded_count = 0
        if not new_state:
            unloaded_count = len(loaded_models)
            loaded_models.clear()
            import gc
            gc.collect()
            logger.info(f"AI service disabled: unloaded {unloaded_count} model(s) from RAM.")
        else:
            logger.info("AI service enabled.")

        try:
            save_server_config({"ai": {"enabled": new_state}})
        except Exception as e:
            logger.warning(f"Failed to persist AI state to server.yaml: {e}")

        return {
            "status": "success",
            "action_executed": "start" if new_state else "stop",
            "models_unloaded": unloaded_count,
            "service_status": get_service_status("ai")
        }, 200

    if action == "toggle":
        curr = get_service_status(service_name)
        action = "stop" if curr.get("is_active") else "start"

    try:
        res = subprocess.run(
            ["sudo", "systemctl", action, service_name],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if res.returncode != 0:
            return {"status": "error", "error": f"systemctl {action} failed: {res.stderr.strip()}"}, 500

        time.sleep(0.5)
        new_status = get_service_status(service_name)
        return {
            "status": "success",
            "action_executed": action,
            "service_status": new_status
        }, 200
    except Exception as e:
        return {"status": "error", "error": str(e)}, 500

# ==============================================================================
# Flask Application & Endpoints
# ==============================================================================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "opi-rv2-ducklake-secret-key-2026")
app.config["MAX_CONTENT_LENGTH"] = CONFIG["storage"].get("max_upload_size_mb", 250) * 1024 * 1024

@app.after_request
def apply_cors_and_headers(response):
    if CONFIG["server"].get("cors_enabled", True):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
    return response

# ------------------------------------------------------------------------------
# 1. Web Console & Status
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    """Serves the unified DuckLake Analytics & AI Web Console."""
    client_ip = get_client_ip()
    ai_allowed = is_ip_allowed_for_ai(client_ip)
    return render_template(
        "index.html",
        client_ip=client_ip,
        ai_allowed=ai_allowed,
        allowed_cidrs=CONFIG["security"]["ai_allowed_cidrs"],
    )

@app.route("/status", methods=["GET"])
def get_status():
    """System health check, storage statistics, and network authorization status."""
    client_ip = get_client_ip()
    ai_allowed = is_ip_allowed_for_ai(client_ip)

    # Disk usage
    lake_stat = shutil.disk_usage(CONFIG["storage"]["ducklake_dir"])
    
    # PostgreSQL health & counts
    pg_ok = False
    project_count = 0
    table_count = 0
    raw_count = 0
    model_count = 0
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM ducklake_projects;")
                project_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM ducklake_tables;")
                table_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM ducklake_raw_files;")
                raw_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM ai_models;")
                model_count = cur.fetchone()[0]
                pg_ok = True
    except Exception as e:
        logger.debug(f"Status check PG error: {e}")

    return jsonify({
        "status": "online",
        "service": "DuckLake & AI Server",
        "architecture": "riscv64",
        "duckdb_version": duckdb.__version__,
        "onnx_available": OG_AVAILABLE,
        "postgres_connected": pg_ok,
        "client": {
            "ip": client_ip,
            "ai_inference_allowed": ai_allowed,
            "required_ai_network": CONFIG["security"]["ai_allowed_cidrs"],
        },
        "storage": {
            "ducklake_dir": CONFIG["storage"]["ducklake_dir"],
            "warehouse_dir": CONFIG["storage"]["warehouse_dir"],
            "projects_count": project_count,
            "parquet_tables_count": table_count,
            "raw_warehouse_files_count": raw_count,
            "registered_models_count": model_count,
            "disk_free_gb": round(lake_stat.free / (1024 ** 3), 2),
            "disk_total_gb": round(lake_stat.total / (1024 ** 3), 2),
        },
        "services": {
            "adminer": get_service_status("adminer"),
            "ai": get_service_status("ai")
        }
    })

@app.route("/services/adminer", methods=["GET", "POST"])
def adminer_service_control():
    """Controls Adminer service (toggle running state to save memory)."""
    if request.method == "GET":
        return jsonify(get_service_status("adminer"))
    elif request.method == "POST":
        body = request.get_json(silent=True) or {}
        action = body.get("action", "toggle").lower()
        res, code = set_service_state("adminer", action)
        return jsonify(res), code

@app.route("/services/ai", methods=["GET", "POST"])
def ai_service_control():
    """Controls AI inference service (toggle/start/stop to unload ONNX models and save memory)."""
    if request.method == "GET":
        return jsonify(get_service_status("ai"))
    elif request.method == "POST":
        body = request.get_json(silent=True) or {}
        action = body.get("action", "toggle").lower()
        res, code = set_service_state("ai", action)
        return jsonify(res), code

# ------------------------------------------------------------------------------
# Configuration Settings API
# ------------------------------------------------------------------------------
@app.route("/config", methods=["GET", "PATCH"])
def server_config_endpoint():
    """Retrieves or dynamically updates server configuration parameters."""
    global CONFIG
    if request.method == "GET":
        sanitized = json.loads(json.dumps(CONFIG))
        if "postgres" in sanitized and "password" in sanitized["postgres"]:
            sanitized["postgres"]["password"] = "******" if sanitized["postgres"]["password"] else ""
        return jsonify({"status": "success", "config": sanitized})

    elif request.method == "PATCH":
        body = request.get_json(silent=True) or {}
        if not body or not isinstance(body, dict):
            return jsonify({"status": "error", "error": "JSON body must be an object of sections to update"}), 400

        if "postgres" in body and isinstance(body["postgres"], dict) and "password" in body["postgres"]:
            del body["postgres"]["password"]

        try:
            CONFIG = save_server_config(body)
            # Reapply engine pragmas
            ducklake_engine.config = CONFIG
            ducklake_engine._apply_pragmas()

            sanitized = json.loads(json.dumps(CONFIG))
            if "postgres" in sanitized and "password" in sanitized["postgres"]:
                sanitized["postgres"]["password"] = "******" if sanitized["postgres"]["password"] else ""

            return jsonify({
                "status": "success",
                "message": "Configuration updated and applied successfully.",
                "config": sanitized
            })
        except Exception as e:
            logger.error(f"Failed to update configuration: {e}")
            return jsonify({"status": "error", "error": str(e)}), 500

# ------------------------------------------------------------------------------
# Projects Partition Management API
# ------------------------------------------------------------------------------
@app.route("/projects", methods=["GET", "POST"])
def projects_collection():
    """List all projects with stats, or register a new project partition."""
    if request.method == "GET":
        projects = ducklake_engine.list_projects()
        return jsonify({"status": "success", "projects": projects})
    
    elif request.method == "POST":
        body = request.get_json(silent=True) or {}
        project_id = body.get("project_id", "").strip()
        display_name = body.get("display_name", "").strip() or project_id
        description = body.get("description", "").strip()
        
        if not project_id:
            return jsonify({"status": "error", "error": "'project_id' is required"}), 400
            
        res = ducklake_engine.create_project(project_id, display_name, description)
        status_code = 200 if res.get("status") == "success" else 400
        return jsonify(res), status_code

@app.route("/projects/<project_id>", methods=["GET", "DELETE"])
def project_item(project_id):
    """Retrieve details or delete a project partition."""
    if request.method == "GET":
        data = ducklake_engine.get_project(project_id)
        if not data:
            return jsonify({"status": "error", "error": f"Project '{project_id}' not found"}), 404
        return jsonify({"status": "success", "project": data})
        
    elif request.method == "DELETE":
        purge = request.args.get("purge", "false").lower() in {"true", "1", "yes"}
        res, code = ducklake_engine.delete_project(project_id, purge_files=purge)
        return jsonify(res), code

@app.route("/projects/<project_id>/upload", methods=["POST"])
def project_upload(project_id):
    """Upload data directly into a project partition (DuckLake table or Warehouse raw file)."""
    if "file" not in request.files:
        return jsonify({"status": "error", "error": "No 'file' field in multipart payload"}), 400
    file_storage = request.files["file"]
    if not file_storage or not file_storage.filename:
        return jsonify({"status": "error", "error": "Uploaded file has no filename"}), 400
        
    target = request.form.get("target", "lake").lower()
    table_name = request.form.get("table_name", "").strip() or None
    mode = request.form.get("mode", "replace").lower()
    
    ducklake_engine.create_project(project_id)
    
    if target == "warehouse":
        res = ducklake_engine.save_warehouse_file(file_storage, file_storage.filename, subfolder=project_id, overwrite=True, project_id=project_id)
    else:
        res = ducklake_engine.ingest_file_to_ducklake(file_storage, file_storage.filename, table_name=table_name, mode=mode, project_id=project_id)
        
    code = 200 if res.get("status") == "success" else 400
    return jsonify(res), code

@app.route("/projects/<project_id>/tables/<table_name>", methods=["PUT", "PATCH"])
def project_table_endpoint(project_id, table_name):
    """RESTful upload/replace (PUT) or append records (PATCH) for a project table."""
    ducklake_engine.create_project(project_id)
    if request.method == "PUT":
        if "file" in request.files:
            file_storage = request.files["file"]
            res = ducklake_engine.ingest_file_to_ducklake(file_storage, file_storage.filename, table_name=table_name, mode="replace", project_id=project_id)
            code = 200 if res.get("status") == "success" else 400
            return jsonify(res), code
        else:
            body = request.get_json(silent=True)
            if isinstance(body, list):
                res = ducklake_engine.insert_records(table_name, body, mode="replace", project_id=project_id)
                code = 200 if res.get("status") == "success" else 400
                return jsonify(res), code
            return jsonify({"status": "error", "error": "Provide multipart 'file' or JSON array of records"}), 400

    elif request.method == "PATCH":
        body = request.get_json(silent=True)
        if not isinstance(body, list):
            return jsonify({"status": "error", "error": "Body must be a JSON array of record objects"}), 400
        res = ducklake_engine.insert_records(table_name, body, mode="append", project_id=project_id)
        code = 200 if res.get("status") == "success" else 400
        return jsonify(res), code

@app.route("/projects/<project_id>/files/<path:filename>", methods=["PUT", "DELETE"])
def project_file_endpoint(project_id, filename):
    """RESTful upload (PUT) or delete (DELETE) a raw warehouse file in a project partition."""
    ducklake_engine.create_project(project_id)
    if request.method == "PUT":
        if "file" in request.files:
            file_storage = request.files["file"]
            res = ducklake_engine.save_warehouse_file(file_storage, filename, subfolder=project_id, overwrite=True, project_id=project_id)
            code = 200 if res.get("status") == "success" else 400
            return jsonify(res), code
        else:
            raw_data = request.get_data(as_text=True)
            res = ducklake_engine.write_warehouse_text(f"{project_id}/{filename}", raw_data, mode="overwrite")
            code = 200 if res.get("status") == "success" else 400
            return jsonify(res), code
    elif request.method == "DELETE":
        res, code = ducklake_engine.delete_warehouse_file(f"{project_id}/{filename}")
        return jsonify(res), code

# ------------------------------------------------------------------------------
# AI Models Listing & Sync API
# ------------------------------------------------------------------------------
@app.route("/ai/models", methods=["GET"])
def detailed_models_endpoint():
    """Returns rich AI models catalog (accessible to Web Console and dashboard clients)."""
    models = get_detailed_models_list()
    return jsonify({"status": "success", "models": models})

@app.route("/ai/models/sync", methods=["POST"])
def sync_models_endpoint():
    """Forces rescanning of storage.models_dir and updates PostgreSQL catalog."""
    sync_models_to_db()
    models = get_detailed_models_list()
    return jsonify({"status": "success", "count": len(models), "models": models})

# ------------------------------------------------------------------------------
# 2. Local AI Endpoints (Gated to 10.0.2.0/24 - OpenAI Compatible)
# ------------------------------------------------------------------------------
@app.route("/v1/models", methods=["GET"])
@require_ai_network
def list_models():
    """Retrieves registered ONNX models from PostgreSQL catalog (OpenAI-compatible)."""
    if not CONFIG["ai"].get("enabled", True):
        return jsonify({"object": "list", "data": [], "warning": "AI inference service is currently disabled to free memory"}), 200

    sync_models_to_db()
    data = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, owned_by FROM ai_models ORDER BY id ASC;")
                rows = cur.fetchall()
                for r in rows:
                    data.append({
                        "id": r[0],
                        "object": "model",
                        "owned_by": r[1] or "local",
                        "permission": []
                    })
    except Exception as e:
        return jsonify({"error": {"message": f"Database error: {e}", "type": "internal_error"}}), 500

    return jsonify({"object": "list", "data": data})

@app.route("/v1/chat/completions", methods=["POST"])
@require_ai_network
def chat_completions():
    """OpenAI-compatible chat completions endpoint (supports SSE streaming)."""
    if not CONFIG["ai"].get("enabled", True):
        return jsonify({"error": {"message": "AI inference service is currently disabled to free memory. Enable it via /services/ai or the web console.", "type": "service_unavailable"}}), 503

    if not OG_AVAILABLE:
        return jsonify({"error": {"message": "ONNX Runtime GenAI is not available", "type": "system_error"}}), 503

    body = request.json or {}
    model_id = body.get("model")
    messages = body.get("messages", [])
    stream = body.get("stream", False)
    max_tokens = body.get("max_tokens", CONFIG["ai"].get("default_max_tokens", 256))
    temperature = body.get("temperature", CONFIG["ai"].get("default_temperature", 0.7))
    client_ip = get_client_ip()

    if not model_id:
        return jsonify({"error": {"message": "Model ID is required", "type": "invalid_request_error"}}), 400

    instance = load_model_instance(model_id)
    if not instance:
        return jsonify({"error": {"message": f"Model '{model_id}' not found", "type": "invalid_request_error"}}), 404

    model = instance["model"]
    tokenizer = instance["tokenizer"]

    # Construct standard ChatML prompt template
    prompt = ""
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role in {"system", "user", "assistant"}:
            prompt += f"<|im_start|>{role}\n{content}<|im_end|>\n"
            log_chat_interaction(model_id, role, content, client_ip)
    prompt += "<|im_start|>assistant\n"

    tokens = tokenizer.encode(prompt)
    context_limit = CONFIG["ai"].get("default_context_length", 2048)
    total_max_len = min(len(tokens) + max_tokens, context_limit)

    params = og.GeneratorParams(model)
    params.set_search_options(
        max_length=total_max_len,
        temperature=temperature,
        repetition_penalty=CONFIG["ai"].get("repetition_penalty", 1.1)
    )
    params.input_ids = tokens

    if stream:
        def generate():
            generator = og.Generator(model, params)
            full_response = ""
            while not generator.is_done():
                generator.compute_logits()
                generator.generate_next_token()
                new_token = generator.get_next_tokens()[0]
                token_text = tokenizer.decode(new_token)
                full_response += token_text

                chunk = {
                    "choices": [{
                        "index": 0,
                        "delta": {"content": token_text},
                        "finish_reason": None
                    }]
                }
                yield f"data: {json.dumps(chunk)}\n\n"

            log_chat_interaction(model_id, "assistant", full_response, client_ip, len(tokens), len(tokenizer.encode(full_response)))
            yield "data: [DONE]\n\n"

        return Response(stream_with_context(generate()), content_type="text/event-stream")

    # Non-streaming inference
    generator = og.Generator(model, params)
    output_text = ""
    while not generator.is_done():
        generator.compute_logits()
        generator.generate_next_token()
        new_token = generator.get_next_tokens()[0]
        output_text += tokenizer.decode(new_token)

    completion_tokens = len(tokenizer.encode(output_text))
    log_chat_interaction(model_id, "assistant", output_text, client_ip, len(tokens), completion_tokens)

    response_payload = {
        "id": "chatcmpl-local",
        "object": "chat.completion",
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": output_text},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": len(tokens),
            "completion_tokens": completion_tokens,
            "total_tokens": len(tokens) + completion_tokens
        }
    }
    return jsonify(response_payload)

# ------------------------------------------------------------------------------
# 3. DuckLake Analytics Endpoints (Cluster-Wide Access)
# ------------------------------------------------------------------------------
@app.route("/ducklake/catalog", methods=["GET"])
@app.route("/ducklake/tables", methods=["GET"])
def ducklake_catalog():
    """Lists registered Parquet tables in DuckLake and raw files in Warehouse."""
    tables = []
    raw_files = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name, source_path, format, row_count, file_size_bytes, schema_info, updated_at
                    FROM ducklake_tables ORDER BY table_name ASC;
                """)
                for r in cur.fetchall():
                    tables.append({
                        "table_name": r[0],
                        "source_path": r[1],
                        "format": r[2],
                        "row_count": r[3],
                        "file_size_bytes": r[4],
                        "schema": r[5],
                        "updated_at": r[6].isoformat() if r[6] else None
                    })

                cur.execute("""
                    SELECT file_name, file_path, file_format, file_size_bytes, status, schema_info, last_scanned
                    FROM ducklake_raw_files ORDER BY file_name ASC;
                """)
                for r in cur.fetchall():
                    raw_files.append({
                        "file_name": r[0],
                        "file_path": r[1],
                        "format": r[2],
                        "file_size_bytes": r[3],
                        "status": r[4],
                        "schema": r[5],
                        "last_scanned": r[6].isoformat() if r[6] else None
                    })
    except Exception as e:
        return jsonify({"status": "error", "message": f"Database catalog error: {e}"}), 500

    return jsonify({
        "status": "success",
        "ducklake_directory": CONFIG["storage"]["ducklake_dir"],
        "warehouse_directory": CONFIG["storage"]["warehouse_dir"],
        "tables": tables,
        "raw_files": raw_files
    })

@app.route("/ducklake/query", methods=["POST"])
def ducklake_query():
    """Executes analytical SQL query on DuckDB across Parquet lake and Warehouse datasets."""
    body = request.json or {}
    sql = body.get("query", "").strip()
    if not sql:
        return jsonify({"status": "error", "error": "Query string is required"}), 400

    client_ip = get_client_ip()
    result = ducklake_engine.execute_query(sql, client_ip)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code

@app.route("/ducklake/sync", methods=["POST", "GET"])
def ducklake_sync():
    """Forces rescanning and cataloging of /srv/DuckLake and /srv/Warehouse."""
    stats = ducklake_engine.scan_and_sync()
    return jsonify({"status": "success", "synced": stats})

@app.route("/ducklake/convert", methods=["POST"])
def ducklake_convert():
    """Converts a raw warehouse dataset (e.g. CSV) to high-speed Parquet in DuckLake."""
    body = request.json or {}
    source_file = body.get("source_file", "").strip()
    table_name = body.get("table_name", "").strip() or None

    if not source_file:
        return jsonify({"status": "error", "error": "source_file parameter is required"}), 400

    res = ducklake_engine.convert_raw_to_parquet(source_file, table_name)
    status_code = 200 if res.get("status") == "success" else 400
    return jsonify(res), status_code

@app.route("/ducklake/queries", methods=["GET"])
def ducklake_queries_history():
    """Returns recent query logs from PostgreSQL."""
    limit = min(int(request.args.get("limit", 50)), 200)
    history = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, query_text, client_ip, execution_time_ms, row_count, status, error_message, created_at
                    FROM ducklake_queries ORDER BY id DESC LIMIT %s;
                """, (limit,))
                for r in cur.fetchall():
                    history.append({
                        "id": r[0],
                        "query": r[1],
                        "client_ip": r[2],
                        "execution_time_ms": r[3],
                        "row_count": r[4],
                        "status": r[5],
                        "error_message": r[6],
                        "created_at": r[7].isoformat() if r[7] else None
                    })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

    return jsonify({"status": "success", "history": history})

# ------------------------------------------------------------------------------
# DuckLake Guide Operations (Data Inlining Flush, Compaction, Snapshots, SDK)
# ------------------------------------------------------------------------------
@app.route("/ducklake_client.py", methods=["GET"])
@app.route("/client/python", methods=["GET"])
def download_python_client():
    """Serves the standalone DuckLake & Warehouse Python Client SDK."""
    sdk_path = Path(__file__).resolve().parent / "ducklake_client.py"
    if not sdk_path.exists():
        return jsonify({"status": "error", "error": "Client module ducklake_client.py not found"}), 404
    return send_file(
        str(sdk_path),
        mimetype="text/x-python",
        as_attachment=True,
        download_name="ducklake_client.py"
    )

@app.route("/ducklake/flush", methods=["POST"])
def ducklake_flush():
    """Flushes inlined records buffer into Parquet storage (Chapter 3: CALL ducklake_flush_inlined_data)."""
    body = request.json or {}
    t_name = body.get("table_name")
    p_id = body.get("project_id")
    res = ducklake_engine.flush_inlined_data(t_name, p_id)
    return jsonify(res)

@app.route("/projects/<project_id>/flush", methods=["POST"])
def project_flush(project_id):
    """Flushes inlined records for a specific project partition."""
    body = request.json or {}
    t_name = body.get("table_name")
    res = ducklake_engine.flush_inlined_data(t_name, project_id)
    return jsonify(res)

@app.route("/ducklake/compact", methods=["POST"])
def ducklake_compact():
    """Compacts Parquet tables to optimize row group sizing and compression (Chapter 5: CALL ducklake_merge_adjacent_files)."""
    body = request.json or {}
    t_name = body.get("table_name")
    p_id = body.get("project_id")
    res = ducklake_engine.merge_adjacent_files(t_name, p_id)
    return jsonify(res)

@app.route("/projects/<project_id>/compact", methods=["POST"])
def project_compact(project_id):
    """Compacts Parquet tables for a specific project partition."""
    body = request.json or {}
    t_name = body.get("table_name")
    res = ducklake_engine.merge_adjacent_files(t_name, project_id)
    return jsonify(res)

@app.route("/ducklake/cleanup", methods=["POST"])
def ducklake_cleanup():
    """Physically removes unreferenced/orphaned storage files (Chapter 5: CALL ducklake_cleanup_old_files)."""
    res = ducklake_engine.cleanup_old_files()
    return jsonify(res)

@app.route("/ducklake/snapshots", methods=["GET"])
def ducklake_snapshots():
    """Returns transactional snapshot history (Chapter 4: FROM snapshots())."""
    snaps = ducklake_engine.get_snapshots()
    return jsonify({"status": "success", "snapshots": snaps})

@app.route("/ducklake/options", methods=["GET", "POST"])
def ducklake_options():
    """Get or set DuckLake configuration options (Chapter 5: CALL set_option)."""
    if request.method == "GET":
        try:
            with get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT option_name, option_value, updated_at FROM ducklake_options ORDER BY option_name;")
                    rows = cur.fetchall()
                    opts = {r[0]: r[1] for r in rows}
                    return jsonify({"status": "success", "options": opts})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500
    else:
        body = request.json or {}
        name = body.get("option_name")
        val = body.get("option_value")
        if not name or val is None:
            return jsonify({"status": "error", "error": "option_name and option_value required"}), 400
        ducklake_engine.set_option(name, str(val))
        return jsonify({"status": "success", "updated": {name: str(val)}})

# ------------------------------------------------------------------------------
# 4. RESTful DuckLake Table Endpoints (GET, PUT, PATCH, DELETE)
# ------------------------------------------------------------------------------
@app.route("/ducklake/tables/<table_name>", methods=["GET", "PUT", "PATCH", "DELETE"])
def ducklake_table_resource(table_name):
    """
    RESTful Resource endpoint for a DuckLake Parquet Table:
      - GET    : Read table schema and preview records (or download parquet if requested).
      - PUT    : Idempotently create or replace the table with uploaded data or records.
      - PATCH  : Append new records or dataset rows to the existing table.
      - DELETE : Delete the table and remove its Parquet file and catalog metadata.
    """
    if request.method == "GET":
        accept_header = request.headers.get("Accept", "").lower()
        if "application/octet-stream" in accept_header or "application/x-parquet" in accept_header:
            return ducklake_table_download(table_name)

        try:
            limit = int(request.args.get("limit", 50))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return jsonify({"status": "error", "error": "limit and offset must be integers"}), 400

        cols_arg = request.args.get("columns")
        columns = [c.strip() for c in cols_arg.split(",") if c.strip()] if cols_arg else None
        result = ducklake_engine.get_table_preview(table_name, limit, offset, columns)
        status_code = 200 if result.get("status") == "success" else 404
        return jsonify(result), status_code

    elif request.method in {"PUT", "PATCH"}:
        mode = "replace" if request.method == "PUT" else "append"
        content_type = request.headers.get("Content-Type", "").lower()

        # 1. Multipart Form Upload
        if "multipart/form-data" in content_type and "file" in request.files:
            file_storage = request.files["file"]
            if not file_storage or not file_storage.filename:
                return jsonify({"status": "error", "error": "Uploaded file has no filename"}), 400
            result = ducklake_engine.ingest_file_to_ducklake(file_storage, file_storage.filename, table_name, mode)
            status_code = 201 if (request.method == "PUT" and result.get("status") == "success") else (200 if result.get("status") == "success" else 400)
            return jsonify(result), status_code

        # 2. JSON Body (Array of records or {"records": [...]})
        elif "application/json" in content_type:
            body = request.get_json(silent=True)
            if body is None:
                return jsonify({"status": "error", "error": "Invalid or missing JSON payload"}), 400
            if isinstance(body, list):
                records = body
            elif isinstance(body, dict):
                records = body.get("records") or body.get("data")
            else:
                records = None

            if not records or not isinstance(records, list):
                return jsonify({"status": "error", "error": "JSON payload must be an array of objects or contain a 'records' list"}), 400

            result = ducklake_engine.insert_records(table_name, records, mode)
            status_code = 201 if (request.method == "PUT" and result.get("status") == "success") else (200 if result.get("status") == "success" else 400)
            return jsonify(result), status_code

        # 3. Raw Data Stream (CSV, TSV, Parquet, JSON lines)
        else:
            raw_data = request.get_data()
            if not raw_data:
                return jsonify({"status": "error", "error": "Empty request body for table creation/update"}), 400

            ext = ".csv"
            if "csv" in content_type:
                ext = ".csv"
            elif "tsv" in content_type or "tab-separated" in content_type:
                ext = ".tsv"
            elif "json" in content_type:
                ext = ".json"
            elif "parquet" in content_type or raw_data.startswith(b"PAR1"):
                ext = ".parquet"

            temp_fd, temp_path = tempfile.mkstemp(prefix="raw_stream_", suffix=ext)
            try:
                with os.fdopen(temp_fd, "wb") as f:
                    f.write(raw_data)

                class MockStorage:
                    def __init__(self, p):
                        self.p = p
                    def save(self, dst):
                        shutil.copy2(self.p, dst)

                result = ducklake_engine.ingest_file_to_ducklake(MockStorage(temp_path), f"{table_name}{ext}", table_name, mode)
                status_code = 201 if (request.method == "PUT" and result.get("status") == "success") else (200 if result.get("status") == "success" else 400)
                return jsonify(result), status_code
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)

    elif request.method == "DELETE":
        result, status_code = ducklake_engine.delete_table(table_name)
        return jsonify(result), status_code

@app.route("/ducklake/tables/<table_name>/download", methods=["GET"])
def ducklake_table_download(table_name):
    """Downloads the raw .parquet file for a DuckLake table."""
    sanitized = ducklake_engine.sanitize_table_name(table_name)
    target = ducklake_engine.ducklake_dir / f"{sanitized}.parquet"
    if not target.exists():
        return jsonify({"status": "error", "error": f"Table '{table_name}' does not exist in DuckLake"}), 404
    return send_file(
        str(target.resolve()),
        as_attachment=True,
        download_name=f"{sanitized}.parquet",
        mimetype="application/octet-stream"
    )

# ------------------------------------------------------------------------------
# 5. RESTful Warehouse File Endpoints (GET, PUT, DELETE)
# ------------------------------------------------------------------------------
@app.route("/warehouse/files", methods=["GET"])
def warehouse_files_list():
    """Lists all raw files cataloged in the Warehouse."""
    files = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT file_name, file_path, file_format, file_size_bytes, status, schema_info, last_scanned
                    FROM ducklake_raw_files ORDER BY file_name ASC;
                """)
                for r in cur.fetchall():
                    files.append({
                        "file_name": r[0],
                        "file_path": r[1],
                        "format": r[2],
                        "file_size_bytes": r[3],
                        "status": r[4],
                        "schema": r[5] if isinstance(r[5], list) else json.loads(r[5] or "[]"),
                        "last_scanned": r[6].isoformat() if r[6] else None
                    })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

    return jsonify({
        "status": "success",
        "warehouse_directory": str(ducklake_engine.warehouse_dir),
        "files": files
    })

@app.route("/warehouse/files/<path:filename>", methods=["GET", "PUT", "DELETE"])
def warehouse_file_resource(filename):
    """
    RESTful Resource endpoint for a Raw Warehouse File:
      - GET    : Read/download file, or preview text/JSON with ?preview=true.
      - PUT    : Idempotently create or replace the file at this exact URI.
      - DELETE : Delete the file at this exact URI.
    """
    if request.method == "GET":
        preview_mode = request.args.get("preview", "false").lower() in {"true", "1", "yes"}
        if preview_mode:
            max_lines = int(request.args.get("max_lines", 100))
            res = ducklake_engine.read_warehouse_preview(filename, max_lines)
            status_code = 200 if res.get("status") == "success" else 404
            return jsonify(res), status_code
        else:
            safe_path = resolve_safe_path(ducklake_engine.warehouse_dir, filename)
            if not safe_path or not safe_path.exists() or safe_path.is_dir():
                return jsonify({"status": "error", "error": f"File '{filename}' not found in Warehouse"}), 404
            return send_file(str(safe_path.resolve()), as_attachment=True, download_name=safe_path.name)

    elif request.method == "PUT":
        safe_path = resolve_safe_path(ducklake_engine.warehouse_dir, filename)
        if not safe_path:
            return jsonify({"status": "error", "error": "Invalid filename or path traversal detected"}), 400

        is_new = not safe_path.exists()
        os.makedirs(safe_path.parent, exist_ok=True)
        content_type = request.headers.get("Content-Type", "").lower()

        # 1. Multipart Form Upload
        if "multipart/form-data" in content_type and "file" in request.files:
            file_storage = request.files["file"]
            if not file_storage:
                return jsonify({"status": "error", "error": "No file uploaded in form"}), 400
            file_storage.save(str(safe_path))

        # 2. Raw Body Stream (binary or text)
        else:
            data = request.get_data()
            with open(safe_path, "wb") as f:
                f.write(data)

        ducklake_engine.scan_and_sync()
        status_code = 201 if is_new else 200
        return jsonify({
            "status": "success",
            "message": "File created" if is_new else "File replaced",
            "file_name": safe_path.name,
            "relative_path": str(safe_path.relative_to(ducklake_engine.warehouse_dir)),
            "file_path": str(safe_path),
            "file_size_bytes": safe_path.stat().st_size,
            "format": safe_path.suffix.lower().lstrip(".")
        }), status_code

    elif request.method == "DELETE":
        result, status_code = ducklake_engine.delete_warehouse_file(filename)
        return jsonify(result), status_code

# ------------------------------------------------------------------------------
# Backwards-Compatible Ingestion Command Aliases
# ------------------------------------------------------------------------------
@app.route("/ducklake/upload", methods=["POST"])
def ducklake_upload():
    """Uploads a dataset file into DuckLake (Convenience POST alias for PUT /ducklake/tables/<name>)."""
    if "file" not in request.files:
        return jsonify({"status": "error", "error": "No 'file' field in multipart/form-data payload"}), 400
    file_storage = request.files["file"]
    if not file_storage or not file_storage.filename:
        return jsonify({"status": "error", "error": "Uploaded file has no filename"}), 400

    table_name = request.form.get("table_name")
    project_id = request.form.get("project_id") or request.args.get("project_id")
    mode = request.form.get("mode", "replace").lower()
    if mode not in {"replace", "append"}:
        mode = "replace"

    result = ducklake_engine.ingest_file_to_ducklake(file_storage, file_storage.filename, table_name, mode, project_id=project_id)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code

@app.route("/ducklake/insert", methods=["POST"])
def ducklake_insert():
    """Ingests JSON records into DuckLake (Convenience POST alias for PUT/PATCH /ducklake/tables/<name>)."""
    body = request.json or {}
    table_name = body.get("table_name")
    project_id = body.get("project_id") or request.args.get("project_id")
    records = body.get("records")
    mode = body.get("mode", "append").lower()

    if not table_name:
        return jsonify({"status": "error", "error": "'table_name' is required"}), 400
    if not records or not isinstance(records, list):
        return jsonify({"status": "error", "error": "'records' must be a non-empty array of JSON objects"}), 400
    if mode not in {"append", "replace"}:
        mode = "append"

    result = ducklake_engine.insert_records(table_name, records, mode, project_id=project_id)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code

@app.route("/warehouse/upload", methods=["POST"])
def warehouse_upload():
    """Uploads file into Warehouse (Convenience POST alias for PUT /warehouse/files/<path>)."""
    if "file" not in request.files:
        return jsonify({"status": "error", "error": "No 'file' field in multipart/form-data payload"}), 400
    file_storage = request.files["file"]
    if not file_storage or not file_storage.filename:
        return jsonify({"status": "error", "error": "Uploaded file has no filename"}), 400

    dest_subfolder = request.form.get("destination_path", "")
    project_id = request.form.get("project_id") or request.args.get("project_id")
    overwrite = request.form.get("overwrite", "true").lower() in {"true", "1", "yes"}

    result = ducklake_engine.save_warehouse_file(file_storage, file_storage.filename, dest_subfolder, overwrite, project_id=project_id)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code

@app.route("/warehouse/write", methods=["POST"])
def warehouse_write():
    """Writes raw content into Warehouse (Convenience POST alias for PUT /warehouse/files/<path>)."""
    body = request.json or {}
    filename = body.get("filename")
    content = body.get("content")
    mode = body.get("mode", "overwrite").lower()

    if not filename:
        return jsonify({"status": "error", "error": "'filename' is required"}), 400
    if content is None:
        return jsonify({"status": "error", "error": "'content' is required"}), 400
    if mode not in {"overwrite", "append"}:
        mode = "overwrite"

    result = ducklake_engine.write_warehouse_text(filename, str(content), mode)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code

# ------------------------------------------------------------------------------
# 6. Service Discovery & LLM Agent Manifest
# ------------------------------------------------------------------------------
def generate_service_manifest(client_ip: str) -> Dict[str, Any]:
    """Generates dynamic manifest and OpenAI tool definitions for client applications and local models."""
    ai_allowed = is_ip_allowed_for_ai(client_ip)

    parquet_tables = []
    warehouse_files = []
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name, source_path, row_count, file_size_bytes, schema_info, updated_at
                    FROM ducklake_tables ORDER BY table_name ASC;
                """)
                for r in cur.fetchall():
                    parquet_tables.append({
                        "table_name": r[0],
                        "source_path": r[1],
                        "row_count": r[2],
                        "file_size_bytes": r[3],
                        "columns": r[4] if isinstance(r[4], list) else json.loads(r[4] or "[]"),
                        "sql_query_example": f'SELECT * FROM "{r[0]}" LIMIT 10;'
                    })

                cur.execute("""
                    SELECT file_name, file_path, file_format, file_size_bytes, status, schema_info
                    FROM ducklake_raw_files ORDER BY file_name ASC;
                """)
                for r in cur.fetchall():
                    warehouse_files.append({
                        "file_name": r[0],
                        "file_path": r[1],
                        "file_format": r[2],
                        "file_size_bytes": r[3],
                        "status": r[4],
                        "schema": r[5] if isinstance(r[5], list) else json.loads(r[5] or "[]")
                    })
    except Exception as e:
        logger.warning(f"Error reading catalog for manifest: {e}")

    tools = [
        {
            "type": "function",
            "function": {
                "name": "query_ducklake",
                "description": "Execute an analytical SQL query on DuckLake Parquet tables or DuckDB views. Query runs at memory speed directly over Parquet files in /srv/DuckLake.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "DuckDB SQL statement to execute. E.g., 'SELECT * FROM cluster_nodes WHERE status = \\'active\\';' or 'SHOW TABLES;'."
                        }
                    },
                    "required": ["query"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_table_schema",
                "description": "Get column names, data types, row count, and sample data rows from a DuckLake Parquet table.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the DuckLake table to inspect."
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of preview rows to fetch (default: 10, max: 100)."
                        }
                    },
                    "required": ["table_name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "insert_ducklake_records",
                "description": "Ingest structured records directly into a DuckLake Parquet table. Automatically creates the table if it does not exist, or appends records to it using ZSTD compression.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Target DuckLake table name."
                        },
                        "records": {
                            "type": "array",
                            "items": { "type": "object" },
                            "description": "List of JSON objects representing rows to insert."
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["append", "replace"],
                            "description": "Whether to append to the existing table or replace it (default: 'append')."
                        }
                    },
                    "required": ["table_name", "records"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "write_warehouse_file",
                "description": "Write or append raw text, JSON, or CSV data directly to a file inside the Warehouse (/srv/Warehouse).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "Relative filename or path within the warehouse (e.g., 'metrics/log.jsonl' or 'raw_data.csv')."
                        },
                        "content": {
                            "type": "string",
                            "description": "The string content to write."
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["overwrite", "append"],
                            "description": "Write mode: 'overwrite' to replace file, 'append' to add to end (default: 'overwrite')."
                        }
                    },
                    "required": ["filename", "content"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "read_warehouse_file",
                "description": "Read the contents or preview of a raw file stored in the Warehouse (/srv/Warehouse).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "Relative filename or path inside /srv/Warehouse."
                        },
                        "max_lines": {
                            "type": "integer",
                            "description": "Maximum number of lines to preview (default: 100)."
                        }
                    },
                    "required": ["filename"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "list_warehouse_files",
                "description": "List all raw files currently stored in the Warehouse (/srv/Warehouse) with formats, sizes, and paths.",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "convert_warehouse_to_parquet",
                "description": "Convert a raw warehouse dataset (e.g., CSV or JSON) into a high-performance compressed Parquet file in DuckLake (/srv/DuckLake).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source_file": {
                            "type": "string",
                            "description": "Filename of the raw dataset in the Warehouse."
                        },
                        "table_name": {
                            "type": "string",
                            "description": "Optional custom table name for the created Parquet file."
                        }
                    },
                    "required": ["source_file"]
                }
            }
        }
    ]

    system_prompt = (
        "You are an intelligent data assistant connected to an Orange Pi RV2 DuckLake Server.\n"
        "- DuckLake (/srv/DuckLake) contains high-speed columnar Parquet datasets queried via embedded DuckDB.\n"
        "- Warehouse (/srv/Warehouse) contains raw unstructured and semi-structured files (CSV, JSON, SQLite, text).\n"
        "- Dedicated project partitions allow archiving datasets under /srv/DuckLake/<project> and /srv/Warehouse/<project>.\n"
        "- Metadata, schemas, and catalog inventory are tracked in PostgreSQL.\n\n"
        "URL Prefix Convention:\n"
        "- The '/v1/' prefix is strictly reserved for OpenAI-compatible AI endpoints (/v1/models, /v1/chat/completions).\n"
        "- Analytical DuckLake, raw Warehouse, Projects, and Management endpoints reside at clean root paths (/ducklake/*, /warehouse/*, /projects/*, /config, /status, /manifest).\n\n"
        "RESTful HTTP Conventions:\n"
        "1. PUT is used to idempotently create or replace resources at a specific URI (tables or files).\n"
        "2. PATCH is used for partial updates / appending data to an existing resource.\n"
        "3. GET is used for reading resource schemas, contents, and downloading files.\n"
        "4. DELETE is used for dropping tables and deleting warehouse files.\n"
        "5. POST is reserved strictly for commands and actions (running SQL queries, converting formats, syncing catalog, chat completions).\n\n"
        "Guidelines for Actions:\n"
        "1. For analytical questions and aggregation, use 'query_ducklake' or inspect tables with 'get_table_schema'.\n"
        "2. To store structured records from your analysis, use 'insert_ducklake_records' (PUT / PATCH).\n"
        "3. To upload or replace raw files, use 'write_warehouse_file' (PUT).\n"
        "4. To read raw data, use 'read_warehouse_file' (GET).\n"
        "5. To transform a raw CSV/JSON dataset into an optimized Parquet table, use 'convert_warehouse_to_parquet' (POST)."
    )

    rest_api_guide = {
        "restful_ducklake_table_endpoints": [
            {
                "path": "/ducklake/tables/<table_name>",
                "method": "PUT",
                "description": "RESTful Idempotent Create or Replace Table. Accepts CSV/TSV/JSON/Parquet raw stream, JSON records array, or multipart file.",
                "curl_example": "curl -X PUT http://<host>:5000/ducklake/tables/inventory --data-binary @inventory.csv -H 'Content-Type: text/csv'"
            },
            {
                "path": "/ducklake/tables/<table_name>",
                "method": "PATCH",
                "description": "RESTful Partial Update / Append to Table. Appends new rows to existing Parquet table using UNION ALL BY NAME.",
                "curl_example": "curl -X PATCH http://<host>:5000/ducklake/tables/inventory -H 'Content-Type: application/json' -d '[{\"sku\":\"A104\",\"qty\":10}]'"
            },
            {
                "path": "/ducklake/tables/<table_name>",
                "method": "GET",
                "description": "RESTful Read Table Resource. Returns column schema and preview data rows (or streams Parquet if Accept: application/octet-stream).",
                "query_params": {"limit": "integer (default 50)", "offset": "integer (default 0)", "columns": "comma-separated column list"},
                "curl_example": "curl http://<host>:5000/ducklake/tables/cluster_nodes?limit=10"
            },
            {
                "path": "/ducklake/tables/<table_name>",
                "method": "DELETE",
                "description": "RESTful Delete Table Resource. Deletes Parquet file, drops DuckDB view, and removes catalog metadata.",
                "curl_example": "curl -X DELETE http://<host>:5000/ducklake/tables/old_table"
            },
            {
                "path": "/ducklake/tables/<table_name>/download",
                "method": "GET",
                "description": "Explicit download endpoint for the table's raw .parquet file.",
                "curl_example": "curl -O http://<host>:5000/ducklake/tables/cluster_nodes/download"
            }
        ],
        "restful_warehouse_file_endpoints": [
            {
                "path": "/warehouse/files/<path:filename>",
                "method": "PUT",
                "description": "RESTful Idempotent Create or Replace Raw File at exact URI. Accepts raw binary or text stream, JSON, or multipart.",
                "curl_example": "curl -X PUT http://<host>:5000/warehouse/files/configs/cluster.conf --data-binary @cluster.conf"
            },
            {
                "path": "/warehouse/files/<path:filename>",
                "method": "GET",
                "description": "RESTful Read / Download Raw File at exact URI. Add ?preview=true to view JSON lines of text/CSV files.",
                "query_params": {"preview": "true/false (returns JSON lines if true)", "max_lines": "integer (default 100)"},
                "curl_example": "curl 'http://<host>:5000/warehouse/files/sample_telemetry.csv?preview=true'"
            },
            {
                "path": "/warehouse/files/<path:filename>",
                "method": "DELETE",
                "description": "RESTful Delete Raw File at exact URI. Deletes file and updates PostgreSQL catalog.",
                "curl_example": "curl -X DELETE http://<host>:5000/warehouse/files/temp_data.csv"
            },
            {
                "path": "/warehouse/files",
                "method": "GET",
                "description": "List all raw files stored in the Warehouse collection.",
                "curl_example": "curl http://<host>:5000/warehouse/files"
            }
        ],
        "project_partition_endpoints": [
            {
                "path": "/projects",
                "method": "GET",
                "description": "List all project partitions with DuckLake and Warehouse storage metrics.",
                "curl_example": "curl http://<host>:5000/projects"
            },
            {
                "path": "/projects",
                "method": "POST",
                "content_type": "application/json",
                "description": "Create a new isolated project partition under /srv/DuckLake/<id> and /srv/Warehouse/<id>.",
                "body_example": {"project_id": "my_app", "description": "My application archive"},
                "curl_example": "curl -X POST http://<host>:5000/projects -H 'Content-Type: application/json' -d '{\"project_id\":\"my_app\"}'"
            },
            {
                "path": "/projects/<project_id>",
                "method": "GET",
                "description": "Get details, table list, and file list for a specific project partition.",
                "curl_example": "curl http://<host>:5000/projects/my_app"
            },
            {
                "path": "/projects/<project_id>",
                "method": "DELETE",
                "description": "Delete a project partition and all its tables and files.",
                "curl_example": "curl -X DELETE http://<host>:5000/projects/my_app"
            },
            {
                "path": "/projects/<project_id>/tables/<table_name>",
                "method": "PUT",
                "description": "RESTful Idempotent Create or Replace Parquet Table within project partition.",
                "curl_example": "curl -X PUT http://<host>:5000/projects/my_app/tables/metrics -F 'file=@metrics.csv'"
            },
            {
                "path": "/projects/<project_id>/tables/<table_name>",
                "method": "PATCH",
                "description": "RESTful Partial Update / Append records to an existing project table.",
                "curl_example": "curl -X PATCH http://<host>:5000/projects/my_app/tables/metrics -H 'Content-Type: application/json' -d '[{\"val\":42}]'"
            },
            {
                "path": "/projects/<project_id>/files/<path:filename>",
                "method": "PUT",
                "description": "RESTful Upload or Replace raw file in project warehouse.",
                "curl_example": "curl -X PUT http://<host>:5000/projects/my_app/files/logs/run.log --data-binary @run.log"
            },
            {
                "path": "/projects/<project_id>/files/<path:filename>",
                "method": "DELETE",
                "description": "RESTful Delete raw file from project warehouse.",
                "curl_example": "curl -X DELETE http://<host>:5000/projects/my_app/files/logs/run.log"
            }
        ],
        "command_endpoints_post": [
            {
                "path": "/ducklake/query",
                "method": "POST",
                "content_type": "application/json",
                "description": "Command: Execute analytical SQL query across DuckLake Parquet tables and warehouse datasets.",
                "body_example": {"query": "SELECT * FROM cluster_nodes LIMIT 5;"},
                "curl_example": "curl -X POST http://<host>:5000/ducklake/query -H 'Content-Type: application/json' -d '{\"query\":\"SELECT * FROM cluster_nodes LIMIT 5;\"}'"
            },
            {
                "path": "/ducklake/convert",
                "method": "POST",
                "content_type": "application/json",
                "description": "Command: Convert a raw warehouse dataset (CSV/JSON) into compressed Parquet lake storage.",
                "body_example": {"source_file": "telemetry.csv", "table_name": "telemetry"},
                "curl_example": "curl -X POST http://<host>:5000/ducklake/convert -H 'Content-Type: application/json' -d '{\"source_file\":\"telemetry.csv\"}'"
            },
            {
                "path": "/ducklake/sync",
                "method": "POST",
                "description": "Command: Trigger rescan and synchronization of /srv/DuckLake and /srv/Warehouse.",
                "curl_example": "curl -X POST http://<host>:5000/ducklake/sync"
            },
            {
                "path": "/v1/chat/completions",
                "method": "POST",
                "content_type": "application/json",
                "description": "Command: Generate chat completions from local ONNX LLM (restricted to 10.0.2.0/24).",
                "curl_example": "curl -X POST http://10.0.2.1:5000/v1/chat/completions -H 'Content-Type: application/json' -d '{\"model\":\"qwen2-int4-0.5b\",\"messages\":[{\"role\":\"user\",\"content\":\"Hi\"}]}'"
            }
        ],
        "ducklake_guide_operations": [
            {
                "path": "/ducklake/flush",
                "method": "POST",
                "description": "Flush small inlined records buffer into persistent Parquet storage (Chapter 3: CALL ducklake_flush_inlined_data).",
                "curl_example": "curl -X POST http://<host>:5000/ducklake/flush"
            },
            {
                "path": "/ducklake/compact",
                "method": "POST",
                "description": "Compacts Parquet lake tables to optimize row group size and ZSTD compression (Chapter 5: CALL ducklake_merge_adjacent_files).",
                "curl_example": "curl -X POST http://<host>:5000/ducklake/compact"
            },
            {
                "path": "/ducklake/cleanup",
                "method": "POST",
                "description": "Physically removes obsolete/temporary storage files (Chapter 5: CALL ducklake_cleanup_old_files).",
                "curl_example": "curl -X POST http://<host>:5000/ducklake/cleanup"
            },
            {
                "path": "/ducklake/snapshots",
                "method": "GET",
                "description": "Inspect catalog snapshot transaction history and operations (Chapter 4: FROM snapshots()).",
                "curl_example": "curl http://<host>:5000/ducklake/snapshots"
            },
            {
                "path": "/ducklake_client.py",
                "method": "GET",
                "description": "Download standalone, zero-dependency Python Client SDK module for apps and projects.",
                "curl_example": "curl -O http://<host>:5000/ducklake_client.py"
            }
        ],
        "discovery_endpoints": [
            {
                "path": "/manifest",
                "method": "GET",
                "description": "Self-describing service manifest, dynamic active catalog, and OpenAI tool definitions for LLM agents."
            },
            {
                "path": "/openapi.json",
                "method": "GET",
                "description": "Standard OpenAPI 3.0.3 specification for the entire service."
            }
        ]
    }

    return {
        "service": "Orange Pi RV2 DuckLake & Local AI Server",
        "version": "1.2.0",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "client": {
            "ip": client_ip,
            "ai_inference_allowed": ai_allowed,
            "required_ai_network": CONFIG["security"]["ai_allowed_cidrs"],
            "ducklake_services_allowed": True
        },
        "storage": {
            "ducklake_dir": CONFIG["storage"]["ducklake_dir"],
            "warehouse_dir": CONFIG["storage"]["warehouse_dir"],
            "models_dir": CONFIG["storage"]["models_dir"]
        },
        "system_prompt": system_prompt,
        "active_catalog": {
            "ducklake_parquet_tables": parquet_tables,
            "warehouse_raw_files": warehouse_files
        },
        "tools": tools,
        "rest_api_guide": rest_api_guide
    }

def generate_openapi_spec() -> Dict[str, Any]:
    """Generates OpenAPI 3.0.3 documentation specification for the server."""
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Orange Pi RV2 DuckLake & AI Server API",
            "version": "1.2.0",
            "description": "RESTful Analytical DuckLake (Parquet + DuckDB), raw file Warehouse, and local ONNX GenAI inference engine."
        },
        "paths": {
            "/ducklake/tables/{table_name}": {
                "get": {
                    "summary": "Read table schema and preview records",
                    "parameters": [
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}},
                        {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                        {"name": "columns", "in": "query", "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Table details and preview rows, or Parquet binary stream"}
                    }
                },
                "put": {
                    "summary": "RESTful Create or Replace DuckLake Table",
                    "description": "Idempotently creates or replaces the table. Accepts raw CSV/TSV/JSON/Parquet body, JSON records array, or multipart upload.",
                    "parameters": [
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {"schema": {"type": "array", "items": {"type": "object"}}},
                            "text/csv": {"schema": {"type": "string"}},
                            "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
                            "multipart/form-data": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"file": {"type": "string", "format": "binary"}}
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Table replaced successfully"},
                        "201": {"description": "Table created successfully"}
                    }
                },
                "patch": {
                    "summary": "RESTful Append rows to existing DuckLake Table",
                    "description": "Appends new rows to an existing Parquet table matching schema by column names.",
                    "parameters": [
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {"schema": {"type": "array", "items": {"type": "object"}}},
                            "text/csv": {"schema": {"type": "string"}}
                        }
                    },
                    "responses": {
                        "200": {"description": "Rows appended successfully"}
                    }
                },
                "delete": {
                    "summary": "RESTful Delete DuckLake Table",
                    "description": "Deletes the underlying Parquet file, drops the DuckDB view, and cleans up catalog metadata.",
                    "parameters": [
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Table deleted successfully"},
                        "404": {"description": "Table not found"}
                    }
                }
            },
            "/ducklake/tables/{table_name}/download": {
                "get": {
                    "summary": "Download raw Parquet file",
                    "parameters": [{"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Parquet binary attachment"}}
                }
            },
            "/warehouse/files": {
                "get": {
                    "summary": "List all Warehouse raw files",
                    "responses": {"200": {"description": "List of files with size and format"}}
                }
            },
            "/warehouse/files/{filename}": {
                "get": {
                    "summary": "Download or preview raw warehouse file",
                    "parameters": [
                        {"name": "filename", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "preview", "in": "query", "schema": {"type": "boolean", "default": False}},
                        {"name": "max_lines", "in": "query", "schema": {"type": "integer", "default": 100}}
                    ],
                    "responses": {"200": {"description": "Raw file stream or JSON preview lines"}}
                },
                "put": {
                    "summary": "RESTful Create or Replace Raw File in Warehouse",
                    "description": "Idempotently saves raw file at specified path. Accepts raw binary/text stream or multipart upload.",
                    "parameters": [
                        {"name": "filename", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
                            "text/plain": {"schema": {"type": "string"}},
                            "multipart/form-data": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"file": {"type": "string", "format": "binary"}}
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "File replaced"},
                        "201": {"description": "File created"}
                    }
                },
                "delete": {
                    "summary": "RESTful Delete Raw File from Warehouse",
                    "parameters": [
                        {"name": "filename", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "File deleted successfully"},
                        "404": {"description": "File not found"}
                    }
                }
            },
            "/ducklake/query": {
                "post": {
                    "summary": "Command: Execute analytical SQL query",
                    "description": "Runs DuckDB query across Parquet lake tables and Warehouse datasets.",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"query": {"type": "string"}},
                                    "required": ["query"]
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Query results"}}
                }
            },
            "/ducklake/convert": {
                "post": {
                    "summary": "Command: Convert raw warehouse dataset to Parquet table",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "source_file": {"type": "string"},
                                        "table_name": {"type": "string"}
                                    },
                                    "required": ["source_file"]
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "File converted to Parquet"}}
                }
            },
            "/ducklake/sync": {
                "post": {
                    "summary": "Command: Rescan storage and sync PostgreSQL metadata",
                    "responses": {"200": {"description": "Synchronization summary"}}
                }
            },
            "/v1/models": {
                "get": {
                    "summary": "List AI models (OpenAI format, Restricted to 10.0.2.0/24)",
                    "responses": {"200": {"description": "List of available models"}}
                }
            },
            "/v1/chat/completions": {
                "post": {
                    "summary": "Command: OpenAI-compatible chat completions (Restricted to 10.0.2.0/24)",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "model": {"type": "string"},
                                        "messages": {"type": "array", "items": {"type": "object"}},
                                        "stream": {"type": "boolean"},
                                        "temperature": {"type": "number"}
                                    },
                                    "required": ["model", "messages"]
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Chat completion response"}}
                }
            },
            "/status": {
                "get": {
                    "summary": "Service status, health, and network authorization",
                    "responses": {"200": {"description": "Service status"}}
                }
            },
            "/manifest": {
                "get": {
                    "summary": "Service discovery manifest and LLM agent function tools",
                    "responses": {"200": {"description": "Full manifest, active catalog, and tool schemas"}}
                }
            },
            "/projects": {
                "get": {
                    "summary": "List all project partitions with DuckLake and Warehouse storage stats",
                    "responses": {"200": {"description": "List of projects"}}
                },
                "post": {
                    "summary": "Create a new project partition",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "project_id": {"type": "string"},
                                        "description": {"type": "string"}
                                    },
                                    "required": ["project_id"]
                                }
                            }
                        }
                    },
                    "responses": {"201": {"description": "Project created"}}
                }
            },
            "/projects/{project_id}": {
                "get": {
                    "summary": "Get details, tables, and files for a specific project partition",
                    "parameters": [{"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Project partition details"}}
                },
                "delete": {
                    "summary": "Delete a project partition",
                    "parameters": [{"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "Project partition deleted"}}
                }
            },
            "/projects/{project_id}/tables/{table_name}": {
                "put": {
                    "summary": "Create or replace Parquet table inside project partition",
                    "parameters": [
                        {"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {"200": {"description": "Table replaced"}, "201": {"description": "Table created"}}
                },
                "patch": {
                    "summary": "Append rows to table inside project partition",
                    "parameters": [
                        {"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "table_name", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {"200": {"description": "Rows appended"}}
                }
            },
            "/projects/{project_id}/files/{filename}": {
                "put": {
                    "summary": "Upload or replace raw file inside project warehouse",
                    "parameters": [
                        {"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "filename", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {"200": {"description": "File replaced"}, "201": {"description": "File created"}}
                },
                "delete": {
                    "summary": "Delete raw file from project warehouse",
                    "parameters": [
                        {"name": "project_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        {"name": "filename", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {"200": {"description": "File deleted"}}
                }
            },
            "/services/adminer": {
                "get": {
                    "summary": "Get status of Adminer DB management web service",
                    "responses": {"200": {"description": "Adminer service status"}}
                },
                "post": {
                    "summary": "Control Adminer service state (toggle/start/stop)",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"action": {"type": "string", "enum": ["toggle", "start", "stop"]}}
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Action executed"}}
                }
            },
            "/services/ai": {
                "get": {
                    "summary": "Get status of local AI inference runtime",
                    "responses": {"200": {"description": "AI service runtime status and loaded model memory"}}
                },
                "post": {
                    "summary": "Control AI service state (toggle/start/stop, unloads ONNX models to reclaim RAM)",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"action": {"type": "string", "enum": ["toggle", "start", "stop"]}}
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "Action executed"}}
                }
            },
            "/config": {
                "get": {
                    "summary": "Get active server configuration",
                    "responses": {"200": {"description": "Server configuration"}}
                },
                "patch": {
                    "summary": "Update server configuration dynamically",
                    "responses": {"200": {"description": "Configuration updated"}}
                }
            },
            "/ducklake_client.py": {
                "get": {
                    "summary": "Download standalone Python Client SDK module",
                    "responses": {"200": {"description": "Python SDK script attachment"}}
                }
            },
            "/ducklake/flush": {
                "post": {
                    "summary": "Flush inlined small-record buffers into persistent Parquet storage",
                    "responses": {"200": {"description": "Flush status and rows transferred"}}
                }
            },
            "/ducklake/compact": {
                "post": {
                    "summary": "Compact small adjacent Parquet lake files into optimized single Parquet file",
                    "responses": {"200": {"description": "Compaction status and snapshot ID"}}
                }
            },
            "/ducklake/cleanup": {
                "post": {
                    "summary": "Physically delete orphaned and temporary storage files",
                    "responses": {"200": {"description": "Cleanup summary"}}
                }
            },
            "/ducklake/snapshots": {
                "get": {
                    "summary": "Get catalog snapshot transaction history (FROM snapshots())",
                    "responses": {"200": {"description": "Snapshot list"}}
                }
            },
            "/ducklake/options": {
                "get": {
                    "summary": "List DuckLake tuning configuration options",
                    "responses": {"200": {"description": "DuckLake options"}}
                },
                "post": {
                    "summary": "Set DuckLake tuning option",
                    "responses": {"200": {"description": "Option updated"}}
                }
            }
        }
    }

@app.route("/manifest", methods=["GET"])
@app.route("/service-manifest", methods=["GET"])
@app.route("/ducklake/manifest", methods=["GET"])
def service_manifest():
    """Machine-readable manifest, dynamic catalog, and OpenAI tool definitions for local LLMs."""
    client_ip = get_client_ip()
    manifest_data = generate_service_manifest(client_ip)
    return jsonify(manifest_data)

@app.route("/openapi.json", methods=["GET"])
def openapi_spec():
    """OpenAPI 3.0.3 specification for the DuckLake and AI server."""
    return jsonify(generate_openapi_spec())

# ==============================================================================
# Initialization & CLI Entry Point
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="DuckLake & AI Server for Orange Pi RV2")
    parser.add_argument("--config", type=str, default="server.yaml", help="Path to server.yaml")
    parser.add_argument("--host", type=str, default=None, help="Server host binding")
    parser.add_argument("--port", type=int, default=None, help="Server port binding")
    parser.add_argument("--no-scan", action="store_true", help="Skip initial storage scan")
    args = parser.parse_args()

    # Initialize PostgreSQL metadata schemas
    init_postgres_metadata()
    migrate_legacy_duckdb_models()
    sync_models_to_db()

    # Auto-scan storage on startup
    if not args.no_scan and CONFIG["ducklake"].get("auto_scan_on_startup", True):
        ducklake_engine.scan_and_sync()

    host = args.host or CONFIG["server"].get("host", "0.0.0.0")
    port = args.port or CONFIG["server"].get("port", 5000)
    debug = CONFIG["server"].get("debug", False)

    logger.info("=" * 60)
    logger.info(f"Orange Pi RV2 DuckLake & AI Server starting on http://{host}:{port}")
    logger.info(f"DuckLake Parquet Dir : {CONFIG['storage']['ducklake_dir']}")
    logger.info(f"Warehouse Raw Dir   : {CONFIG['storage']['warehouse_dir']}")
    logger.info(f"AI Allowed Networks : {CONFIG['security']['ai_allowed_cidrs']}")
    logger.info("=" * 60)

    app.run(host=host, port=port, debug=debug)

if __name__ == "__main__":
    main()
