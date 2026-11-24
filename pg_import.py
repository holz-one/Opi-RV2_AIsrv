#!/usr/bin/env python3
"""
pg_import.py - Universal Data Importer for PostgreSQL

Supports importing datasets into PostgreSQL from:
  - SQLite (.sqlite, .db, .sqlite3)
  - DuckDB (.duckdb, .ddb)
  - Parquet (.parquet, .pq)
  - CSV / TSV / Delimited (.csv, .tsv, .txt, .tab)
  - Excel (.xlsx, .xls)
  - LibreOffice Calc (.ods)

Key features:
  - High-performance binary/text streaming using PostgreSQL COPY via psycopg
  - Automatic column type inference & SQLite affinity sanitization
  - Multi-sheet and multi-table support with filtering
  - Modes: replace (default), append, fail
  - Preview & schema inspection without modifying database
  - Identifier sanitization (lowercase, valid SQL identifiers, reserved words safe)
"""

import argparse
import csv
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple


# ==============================================================================
# Terminal Colors & Styling
# ==============================================================================
class Term:
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    @classmethod
    def strip(cls, text: str) -> str:
        return re.sub(r"\033\[[0-9;]*m", "", text)


def log_info(msg: str):
    print(f"{Term.BLUE}[+]{Term.RESET} {msg}")


def log_success(msg: str):
    print(f"{Term.GREEN}[✓]{Term.RESET} {msg}")


def log_warn(msg: str):
    print(f"{Term.YELLOW}[!]{Term.RESET} {msg}")


def log_error(msg: str):
    print(f"{Term.RED}[✗]{Term.RESET} {msg}", file=sys.stderr)


# ==============================================================================
# Identifier Sanitization
# ==============================================================================
def sanitize_identifier(name: str, max_len: int = 63, default_prefix: str = "t") -> str:
    """
    Sanitize an identifier (table or column) to lowercased PostgreSQL standard.
    Truncates to max_len (Postgres limit is 63 chars).
    """
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip().lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = f"{default_prefix}_table"
    if clean[0].isdigit():
        clean = f"{default_prefix}_{clean}"
    return clean[:max_len]


def sanitize_columns(cols: List[str]) -> List[str]:
    """Sanitize and deduplicate column names."""
    cleaned = []
    seen: Dict[str, int] = {}
    for idx, c in enumerate(cols):
        name = sanitize_identifier(c, max_len=63, default_prefix="c")
        if not name:
            name = f"col_{idx + 1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"[:63]
        else:
            seen[name] = 1
        cleaned.append(name)
    return cleaned


# ==============================================================================
# Pretty Table Preview Formatter
# ==============================================================================
def format_ascii_table(headers: List[str], rows: List[List[Any]], max_width: int = 35) -> str:
    """Format sample rows into a clean terminal ASCII table."""
    if not headers:
        return ""
    col_count = len(headers)
    str_rows = []
    for r in rows:
        row_cells = []
        for c in range(col_count):
            val = r[c] if c < len(r) else ""
            if val is None:
                row_cells.append("NULL")
            else:
                s = str(val).replace("\n", " ").replace("\r", "")
                if len(s) > max_width:
                    s = s[: max_width - 3] + "..."
                row_cells.append(s)
        str_rows.append(row_cells)

    widths = [len(h) for h in headers]
    for row in str_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |"

    lines = [border, header_line, border]
    for row in str_rows:
        line = "| " + " | ".join(cell.ljust(w) for cell, w in zip(row, widths)) + " |"
        lines.append(line)
    lines.append(border)
    return "\n".join(lines)


# ==============================================================================
# Base Data Adapter & Concrete Implementations
# ==============================================================================
class BaseAdapter:
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def list_sources(self) -> List[str]:
        """List table or sheet names available in the file."""
        raise NotImplementedError

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        """Return list of (sanitized_column_name, postgres_data_type)."""
        raise NotImplementedError

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        """Return (headers, sample_rows)."""
        raise NotImplementedError

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        """Yield chunks of rows formatted as tuples ready for psycopg copy."""
        raise NotImplementedError

    def get_row_count(self, source_name: str) -> Optional[int]:
        """Return total row count if known, or None."""
        return None

    def close(self):
        pass


class SQLiteAdapter(BaseAdapter):
    SQLITE_TYPE_MAP = {
        "INTEGER": "BIGINT",
        "INT": "BIGINT",
        "TINYINT": "SMALLINT",
        "SMALLINT": "SMALLINT",
        "MEDIUMINT": "INTEGER",
        "BIGINT": "BIGINT",
        "REAL": "DOUBLE PRECISION",
        "DOUBLE": "DOUBLE PRECISION",
        "FLOAT": "DOUBLE PRECISION",
        "NUMERIC": "NUMERIC",
        "DECIMAL": "NUMERIC",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "BLOB": "BYTEA",
        "TEXT": "TEXT",
        "VARCHAR": "TEXT",
        "CHAR": "TEXT",
        "CLOB": "TEXT",
    }

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        import sqlite3

        self.conn = sqlite3.connect(f"file:{file_path.resolve()}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row

    def list_sources(self) -> List[str]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        )
        return [row[0] for row in cur.fetchall()]

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        cur = self.conn.cursor()
        cur.execute(f'PRAGMA table_info("{source_name}");')
        info = cur.fetchall()
        raw_cols = [r[1] for r in info]
        sanitized = sanitize_columns(raw_cols)

        schema = []
        for (_, raw_name, raw_type, _, _, _), col_name in zip(info, sanitized):
            norm_type = raw_type.strip().upper() if raw_type else "TEXT"
            # Strip precision like VARCHAR(255) -> VARCHAR
            base_type = norm_type.split("(")[0].strip()
            pg_type = self.SQLITE_TYPE_MAP.get(base_type, "TEXT")

            # Check for SQLite affinity quirk: declared numeric/integer but holding text
            if pg_type in ("BIGINT", "INTEGER", "SMALLINT", "DOUBLE PRECISION", "NUMERIC"):
                cur.execute(
                    f'SELECT typeof("{raw_name}") FROM "{source_name}" WHERE "{raw_name}" IS NOT NULL LIMIT 50;'
                )
                types_found = {r[0].lower() for r in cur.fetchall()}
                if "text" in types_found:
                    pg_type = "TEXT"

            schema.append((col_name, pg_type))
        return schema

    def get_row_count(self, source_name: str) -> Optional[int]:
        cur = self.conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{source_name}";')
        return cur.fetchone()[0]

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        schema = self.get_schema(source_name)
        headers = [col for col, _ in schema]
        cur = self.conn.cursor()
        cur.execute(f'SELECT * FROM "{source_name}" LIMIT {limit};')
        rows = [list(r) for r in cur.fetchall()]
        return headers, rows

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        cur = self.conn.cursor()
        cur.execute(f'SELECT * FROM "{source_name}";')
        while True:
            batch = cur.fetchmany(chunk_size)
            if not batch:
                break
            yield [tuple(r) for r in batch]

    def close(self):
        self.conn.close()


class DuckDBAdapter(BaseAdapter):
    DUCKDB_TYPE_MAP = {
        "BOOLEAN": "BOOLEAN",
        "TINYINT": "SMALLINT",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "HUGEINT": "NUMERIC",
        "UTINYINT": "SMALLINT",
        "USMALLINT": "INTEGER",
        "UINTEGER": "BIGINT",
        "UBIGINT": "NUMERIC",
        "FLOAT": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "VARCHAR": "TEXT",
        "TEXT": "TEXT",
        "BLOB": "BYTEA",
        "DATE": "DATE",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMPTZ": "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
        "INTERVAL": "INTERVAL",
        "JSON": "JSONB",
        "UUID": "UUID",
    }

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        try:
            import duckdb
        except ImportError:
            raise RuntimeError("DuckDB support requires 'duckdb'. Install via: pip install duckdb")
        self.duckdb = duckdb
        self.conn = duckdb.connect(database=str(file_path.resolve()), read_only=True)

    def list_sources(self) -> List[str]:
        res = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name;"
        ).fetchall()
        return [r[0] for r in res]

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        res = self.conn.execute(f"PRAGMA table_info('{source_name}');").fetchall()
        raw_cols = [r[1] for r in res]
        sanitized = sanitize_columns(raw_cols)

        schema = []
        for (_, raw_name, raw_type, _, _, _), col_name in zip(res, sanitized):
            norm_type = raw_type.strip().upper().split("(")[0].strip()
            pg_type = self.DUCKDB_TYPE_MAP.get(norm_type, "TEXT")
            if "DECIMAL" in norm_type or "NUMERIC" in norm_type:
                pg_type = "NUMERIC"
            schema.append((col_name, pg_type))
        return schema

    def get_row_count(self, source_name: str) -> Optional[int]:
        res = self.conn.execute(f'SELECT COUNT(*) FROM "{source_name}";').fetchone()
        return res[0] if res else 0

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        schema = self.get_schema(source_name)
        headers = [col for col, _ in schema]
        res = self.conn.execute(f'SELECT * FROM "{source_name}" LIMIT {limit};').fetchall()
        return headers, [list(r) for r in res]

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        rel = self.conn.execute(f'SELECT * FROM "{source_name}";')
        while True:
            chunk = rel.fetchmany(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        self.conn.close()


class ParquetAdapter(BaseAdapter):
    """Adapter for Apache Parquet files (.parquet, .pq) using DuckDB."""

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        try:
            import duckdb
        except ImportError:
            raise RuntimeError("Parquet support requires 'duckdb'. Install via: pip install duckdb")
        self.duckdb = duckdb
        self.conn = duckdb.connect()
        self.escaped_path = str(file_path.resolve()).replace("'", "''")

    def list_sources(self) -> List[str]:
        stem = sanitize_identifier(self.file_path.stem)
        return [stem]

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        res = self.conn.execute(f"DESCRIBE SELECT * FROM '{self.escaped_path}';").fetchall()
        raw_cols = [r[0] for r in res]
        sanitized = sanitize_columns(raw_cols)
        schema = []
        for (_, raw_type, _, _, _, _), col_name in zip(res, sanitized):
            norm_type = raw_type.strip().upper().split("(")[0].strip()
            pg_type = DuckDBAdapter.DUCKDB_TYPE_MAP.get(norm_type, "TEXT")
            if "DECIMAL" in norm_type or "NUMERIC" in norm_type:
                pg_type = "NUMERIC"
            schema.append((col_name, pg_type))
        return schema

    def get_row_count(self, source_name: str) -> Optional[int]:
        res = self.conn.execute(f"SELECT COUNT(*) FROM '{self.escaped_path}';").fetchone()
        return res[0] if res else 0

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        schema = self.get_schema(source_name)
        headers = [col for col, _ in schema]
        res = self.conn.execute(f"SELECT * FROM '{self.escaped_path}' LIMIT {limit};").fetchall()
        return headers, [list(r) for r in res]

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        rel = self.conn.execute(f"SELECT * FROM '{self.escaped_path}';")
        while True:
            chunk = rel.fetchmany(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        self.conn.close()


class PandasBaseAdapter(BaseAdapter):
    """Base class for CSV, Excel, and LibreOffice Calc using pandas."""

    @staticmethod
    def map_dtype(dtype) -> str:
        s = str(dtype).lower()
        if "int" in s:
            return "BIGINT"
        elif "float" in s:
            return "DOUBLE PRECISION"
        elif "bool" in s:
            return "BOOLEAN"
        elif "datetime" in s:
            return "TIMESTAMPTZ"
        elif "timedelta" in s:
            return "INTERVAL"
        return "TEXT"

    @staticmethod
    def clean_row(row: Tuple, pd, np) -> Tuple:
        cleaned = []
        for val in row:
            if val is None or val is pd.NA or val is pd.NaT:
                cleaned.append(None)
            elif isinstance(val, float) and (np.isnan(val) or math.isnan(val)):
                cleaned.append(None)
            elif isinstance(val, pd.Timestamp):
                cleaned.append(val.to_pydatetime())
            elif hasattr(val, "item"):
                cleaned.append(val.item())
            else:
                cleaned.append(val)
        return tuple(cleaned)


class CSVAdapter(PandasBaseAdapter):
    def __init__(self, file_path: Path, delimiter: Optional[str] = None, encoding: str = "utf-8", no_header: bool = False):
        super().__init__(file_path)
        try:
            import numpy as np
            import pandas as pd
        except ImportError:
            raise RuntimeError("CSV support requires 'pandas' and 'numpy'. Install via: pip install pandas numpy")
        self.pd = pd
        self.np = np
        self.encoding = encoding
        self.no_header = no_header

        if delimiter:
            self.delimiter = delimiter
        elif file_path.suffix.lower() in (".tsv", ".tab"):
            self.delimiter = "\t"
        else:
            self.delimiter = self._sniff_delimiter(file_path, encoding)

    def _sniff_delimiter(self, file_path: Path, encoding: str) -> str:
        try:
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                sample = f.read(8192)
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
            return dialect.delimiter
        except Exception:
            return ","

    def list_sources(self) -> List[str]:
        stem = sanitize_identifier(self.file_path.stem)
        return [stem]

    def _get_first_chunk(self, chunksize: int = 1000):
        header = None if self.no_header else 0
        return self.pd.read_csv(
            self.file_path,
            sep=self.delimiter,
            encoding=self.encoding,
            header=header,
            nrows=chunksize,
            low_memory=False,
        )

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        df = self._get_first_chunk(chunksize=1000)
        raw_cols = [str(c) for c in df.columns]
        sanitized = sanitize_columns(raw_cols)
        schema = []
        for col_name, dtype in zip(sanitized, df.dtypes):
            schema.append((col_name, self.map_dtype(dtype)))
        return schema

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        df = self._get_first_chunk(chunksize=limit)
        schema = self.get_schema(source_name)
        headers = [col for col, _ in schema]
        rows = [list(r) for r in df.head(limit).to_numpy()]
        return headers, rows

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        header = None if self.no_header else 0
        reader = self.pd.read_csv(
            self.file_path,
            sep=self.delimiter,
            encoding=self.encoding,
            header=header,
            chunksize=chunk_size,
            low_memory=False,
        )
        for chunk in reader:
            batch = [
                self.clean_row(row, self.pd, self.np)
                for row in chunk.itertuples(index=False, name=None)
            ]
            yield batch


class SpreadsheetAdapter(PandasBaseAdapter):
    """Adapter for Excel (.xlsx, .xls) and LibreOffice Calc (.ods)."""

    def __init__(self, file_path: Path, engine: Optional[str] = None):
        super().__init__(file_path)
        try:
            import numpy as np
            import pandas as pd
        except ImportError:
            raise RuntimeError("Spreadsheet support requires 'pandas' and 'numpy'.")
        self.pd = pd
        self.np = np
        self.engine = engine

        ext = file_path.suffix.lower()
        if ext == ".ods":
            self.engine = "odf"
            try:
                import odf
            except ImportError:
                raise RuntimeError("LibreOffice Calc (.ods) support requires 'odfpy'. Install via: pip install odfpy")
        elif ext == ".xlsx":
            self.engine = "openpyxl"
            try:
                import openpyxl
            except ImportError:
                raise RuntimeError("Excel (.xlsx) support requires 'openpyxl'. Install via: pip install openpyxl")
        elif ext == ".xls":
            self.engine = "xlrd"
            try:
                import xlrd
            except ImportError:
                raise RuntimeError("Excel (.xls) support requires 'xlrd'. Install via: pip install xlrd")

        self.excel_file = self.pd.ExcelFile(file_path, engine=self.engine)
        self._cached_dfs: Dict[str, Any] = {}

    def list_sources(self) -> List[str]:
        return self.excel_file.sheet_names

    def _get_df(self, source_name: str):
        if source_name not in self._cached_dfs:
            self._cached_dfs[source_name] = self.excel_file.parse(source_name)
        return self._cached_dfs[source_name]

    def get_schema(self, source_name: str) -> List[Tuple[str, str]]:
        df = self._get_df(source_name)
        raw_cols = [str(c) for c in df.columns]
        sanitized = sanitize_columns(raw_cols)
        schema = []
        for col_name, dtype in zip(sanitized, df.dtypes):
            schema.append((col_name, self.map_dtype(dtype)))
        return schema

    def get_row_count(self, source_name: str) -> Optional[int]:
        df = self._get_df(source_name)
        return len(df)

    def get_preview(self, source_name: str, limit: int = 5) -> Tuple[List[str], List[List[Any]]]:
        df = self._get_df(source_name)
        schema = self.get_schema(source_name)
        headers = [col for col, _ in schema]
        sample = df.head(limit)
        rows = [list(r) for r in sample.to_numpy()]
        return headers, rows

    def stream_rows(self, source_name: str, chunk_size: int = 5000) -> Generator[List[Tuple], None, None]:
        df = self._get_df(source_name)
        num_rows = len(df)
        for start in range(0, num_rows, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            batch = [
                self.clean_row(row, self.pd, self.np)
                for row in chunk.itertuples(index=False, name=None)
            ]
            yield batch


# ==============================================================================
# Adapter Factory
# ==============================================================================
def get_adapter(file_path: Path, forced_format: Optional[str] = None, **kwargs) -> BaseAdapter:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    fmt = forced_format.lower() if forced_format else None
    ext = file_path.suffix.lower()

    if fmt == "sqlite" or (fmt is None and ext in (".sqlite", ".db", ".sqlite3", ".db3")):
        return SQLiteAdapter(file_path)
    elif fmt == "duckdb" or (fmt is None and ext in (".duckdb", ".ddb")):
        return DuckDBAdapter(file_path)
    elif fmt in ("parquet", "pq") or (fmt is None and ext in (".parquet", ".pq")):
        return ParquetAdapter(file_path)
    elif fmt == "csv" or (fmt is None and ext in (".csv", ".tsv", ".txt", ".tab", ".psv")):
        return CSVAdapter(
            file_path,
            delimiter=kwargs.get("delimiter"),
            encoding=kwargs.get("encoding", "utf-8"),
            no_header=kwargs.get("no_header", False),
        )
    elif fmt in ("excel", "xlsx", "xls") or (fmt is None and ext in (".xlsx", ".xls", ".xlsm")):
        return SpreadsheetAdapter(file_path)
    elif fmt == "ods" or (fmt is None and ext == ".ods"):
        return SpreadsheetAdapter(file_path, engine="odf")
    else:
        raise ValueError(
            f"Unsupported file format '{ext}'. Specify format using -f/--format (sqlite, duckdb, parquet, csv, excel, ods)."
        )


# ==============================================================================
# Database Importer Core
# ==============================================================================
def create_pg_connection(args):
    try:
        import psycopg
        from psycopg import sql
    except ImportError:
        raise RuntimeError("PostgreSQL connection requires 'psycopg'. Install via: pip install psycopg")

    if args.url:
        return psycopg.connect(args.url), sql

    conn_kwargs = {}
    if args.dbname:
        conn_kwargs["dbname"] = args.dbname
    if args.user:
        conn_kwargs["user"] = args.user
    if args.password:
        conn_kwargs["password"] = args.password
    if args.host:
        conn_kwargs["host"] = args.host
    if args.port:
        conn_kwargs["port"] = args.port

    return psycopg.connect(**conn_kwargs), sql


def prepare_table(cur, sql_module, schema: str, table: str, columns: List[Tuple[str, str]], mode: str, drop_cascade: bool):
    table_ident = sql_module.Identifier(schema, table)

    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s;",
        (schema, table),
    )
    exists = cur.fetchone() is not None

    if exists:
        if mode == "fail":
            raise RuntimeError(f"Table '{schema}.{table}' already exists and --mode is 'fail'.")
        elif mode == "replace":
            cascade = sql_module.SQL(" CASCADE") if drop_cascade else sql_module.SQL("")
            cur.execute(sql_module.SQL("DROP TABLE IF EXISTS {} {};").format(table_ident, cascade))
        elif mode == "append":
            return

    col_defs = [
        sql_module.SQL("{} {}").format(sql_module.Identifier(c), sql_module.SQL(pg_type))
        for c, pg_type in columns
    ]
    create_query = sql_module.SQL("CREATE TABLE {} ({});").format(
        table_ident, sql_module.SQL(", ").join(col_defs)
    )
    cur.execute(create_query)


def copy_stream(conn, cur, sql_module, schema: str, table: str, columns: List[str], row_chunks: Generator, total_expected: Optional[int], quiet: bool) -> int:
    table_ident = sql_module.Identifier(schema, table)
    cols_ident = sql_module.SQL(", ").join([sql_module.Identifier(c) for c in columns])
    copy_sql = sql_module.SQL("COPY {} ({}) FROM STDIN").format(table_ident, cols_ident)

    total_rows = 0
    start_time = time.time()

    with cur.copy(copy_sql) as copy:
        for chunk in row_chunks:
            for row in chunk:
                copy.write_row(row)
                total_rows += 1
            if not quiet and total_expected and total_expected > 0:
                pct = min(100.0, (total_rows / total_expected) * 100.0)
                elapsed = time.time() - start_time
                rate = total_rows / max(elapsed, 0.001)
                sys.stdout.write(
                    f"\r    {Term.CYAN}↳{Term.RESET} Streaming: {total_rows:,}/{total_expected:,} rows ({pct:.1f}%, {rate:,.0f} rows/s)..."
                )
                sys.stdout.flush()

    conn.commit()
    elapsed = time.time() - start_time
    rate = total_rows / max(elapsed, 0.001)
    if not quiet:
        msg = f"\r    {Term.GREEN}✓{Term.RESET} Inserted {total_rows:,} rows in {elapsed:.2f}s ({rate:,.0f} rows/s)"
        print(msg + " " * 10)
    return total_rows


# ==============================================================================
# CLI Entry Point & Dispatcher
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        prog="pg_import.py",
        description="Universal Data Importer for PostgreSQL (SQLite, DuckDB, Parquet, CSV, Excel, LibreOffice Calc)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview an SQLite file before importing:
  ./pg_import.py data.db --preview

  # Preview a Parquet dataset:
  ./pg_import.py corpus.parquet --preview

  # List sheets in a LibreOffice Calc file:
  ./pg_import.py budget.ods --list

  # Import CSV into database 'orangepi' with custom table name:
  ./pg_import.py sales.csv -d orangepi -t sales_2026

  # Import Parquet file into database 'orangepi':
  ./pg_import.py dataset.parquet -d orangepi -t corpus

  # Import all tables from DuckDB into schema 'analytics':
  ./pg_import.py analytics.duckdb -d orangepi -s analytics

  # Import a specific sheet from Excel, appending to existing table:
  ./pg_import.py reports.xlsx --sheet "Q1 Summary" -t q1_data --mode append
        """,
    )

    # Positional
    parser.add_argument("file", type=Path, help="Path to input data file")

    # PostgreSQL Connection Options
    conn_grp = parser.add_argument_group("PostgreSQL Connection")
    conn_grp.add_argument("-d", "--dbname", "--database", default=os.getenv("PGDATABASE", "orangepi"), help="Target database name (default: orangepi)")
    conn_grp.add_argument("-s", "--schema", default="public", help="Target schema name (default: public)")
    conn_grp.add_argument("-H", "--host", default=os.getenv("PGHOST"), help="PostgreSQL host (default: local UNIX socket)")
    conn_grp.add_argument("-p", "--port", type=int, default=int(os.getenv("PGPORT", "5432")), help="PostgreSQL port (default: 5432)")
    conn_grp.add_argument("-U", "--user", default=os.getenv("PGUSER", "orangepi"), help="Database user (default: orangepi)")
    conn_grp.add_argument("-W", "--password", default=os.getenv("PGPASSWORD"), help="Database password")
    conn_grp.add_argument("--url", help="Complete PostgreSQL connection URI (e.g. postgresql://user:pass@host/db)")

    # Target Mapping
    map_grp = parser.add_argument_group("Target Table Mapping")
    map_grp.add_argument("-t", "--table", help="Target table name (for single table/sheet imports)")
    map_grp.add_argument("--prefix", default="", help="Prefix to prepend to target table names")
    map_grp.add_argument("-m", "--mode", choices=["replace", "append", "fail"], default="replace", help="Import mode: replace (drop & recreate), append, or fail (default: replace)")
    map_grp.add_argument("--no-cascade", action="store_true", help="Do not use CASCADE when dropping tables in replace mode")

    # Source Selection & Parsing
    src_grp = parser.add_argument_group("Source File & Parsing Options")
    src_grp.add_argument("-f", "--format", choices=["sqlite", "duckdb", "parquet", "csv", "excel", "ods"], help="Explicitly specify file format")
    src_grp.add_argument("--source-table", help="Specific table to import from SQLite/DuckDB")
    src_grp.add_argument("--sheet", help="Specific sheet name or 0-based index to import from Excel/ODS")
    src_grp.add_argument("-D", "--delimiter", help="Custom delimiter for CSV/TSV files")
    src_grp.add_argument("--encoding", default="utf-8", help="File encoding for text/CSV files (default: utf-8)")
    src_grp.add_argument("--no-header", action="store_true", help="CSV file has no header row")

    # Execution Modes
    exec_grp = parser.add_argument_group("Execution Modes")
    exec_grp.add_argument("-n", "--preview", action="store_true", help="Preview detected schema and sample rows without modifying database")
    exec_grp.add_argument("-l", "--list", action="store_true", help="List tables or sheets in the source file and exit")
    exec_grp.add_argument("-c", "--chunk-size", type=int, default=5000, help="Batch row chunk size for streaming COPY (default: 5000)")
    exec_grp.add_argument("-q", "--quiet", action="store_true", help="Quiet mode (suppress progress output)")

    args = parser.parse_args()

    # 1. Initialize Adapter
    try:
        adapter = get_adapter(
            args.file,
            forced_format=args.format,
            delimiter=args.delimiter,
            encoding=args.encoding,
            no_header=args.no_header,
        )
    except Exception as e:
        log_error(f"Failed to open source file: {e}")
        sys.exit(1)

    # 2. List mode
    try:
        all_sources = adapter.list_sources()
    except Exception as e:
        log_error(f"Failed to inspect sources: {e}")
        adapter.close()
        sys.exit(1)

    if args.list:
        print(f"\n{Term.BOLD}Sources available in {args.file.name}:{Term.RESET}")
        for s in all_sources:
            count = adapter.get_row_count(s)
            count_str = f"({count:,} rows)" if count is not None else ""
            print(f"  • {s} {Term.DIM}{count_str}{Term.RESET}")
        adapter.close()
        sys.exit(0)

    # Filter sources based on user selection
    sources_to_process = all_sources
    if args.source_table:
        if args.source_table not in all_sources:
            log_error(f"Source table '{args.source_table}' not found in file. Available: {', '.join(all_sources)}")
            adapter.close()
            sys.exit(1)
        sources_to_process = [args.source_table]
    elif args.sheet:
        if args.sheet in all_sources:
            sources_to_process = [args.sheet]
        elif args.sheet.isdigit() and int(args.sheet) < len(all_sources):
            sources_to_process = [all_sources[int(args.sheet)]]
        else:
            log_error(f"Sheet '{args.sheet}' not found. Available sheets: {', '.join(all_sources)}")
            adapter.close()
            sys.exit(1)

    if not sources_to_process:
        log_warn("No tables or sheets found to process.")
        adapter.close()
        sys.exit(0)

    # 3. Preview mode
    if args.preview:
        print(f"\n{Term.BOLD}{'='*80}{Term.RESET}")
        print(f"{Term.BOLD}PREVIEW: {args.file.name} ({adapter.__class__.__name__}){Term.RESET}")
        print(f"{Term.BOLD}{'='*80}{Term.RESET}\n")

        for src in sources_to_process:
            dest_tbl = args.table if (args.table and len(sources_to_process) == 1) else f"{args.prefix}{sanitize_identifier(src)}"
            schema = adapter.get_schema(src)
            row_count = adapter.get_row_count(src)
            count_info = f"{row_count:,} rows" if row_count is not None else "unknown count"

            print(f"{Term.BOLD}Source:{Term.RESET} '{src}'  -->  {Term.BOLD}Target Table:{Term.RESET} '{args.schema}.{dest_tbl}' ({count_info})")
            print(f"{Term.DIM}Columns ({len(schema)}):{Term.RESET}")
            for col, pg_type in schema:
                print(f"  • {col}: {Term.CYAN}{pg_type}{Term.RESET}")
            print()

            headers, sample_rows = adapter.get_preview(src, limit=5)
            if sample_rows:
                print(format_ascii_table(headers, sample_rows))
            else:
                print(f"{Term.DIM}(Source is empty){Term.RESET}")
            print(f"\n{'-'*80}\n")

        adapter.close()
        sys.exit(0)

    # Validation: multiple tables vs explicit table name
    if args.table and len(sources_to_process) > 1:
        log_error("Cannot use --table with multiple source tables/sheets. Use --prefix or specify --source-table/--sheet.")
        adapter.close()
        sys.exit(1)

    # 4. Connect to PostgreSQL
    if not args.quiet:
        conn_target = args.url or f"dbname={args.dbname} host={args.host or 'local socket'} port={args.port} user={args.user}"
        log_info(f"Connecting to PostgreSQL ({conn_target})...")

    try:
        conn, sql_module = create_pg_connection(args)
    except Exception as e:
        log_error(f"Failed to connect to PostgreSQL: {e}")
        adapter.close()
        sys.exit(1)

    total_imported_all = 0
    start_total_time = time.time()

    try:
        with conn.cursor() as cur:
            # Ensure target schema exists
            cur.execute(
                sql_module.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql_module.Identifier(args.schema))
            )
            conn.commit()

            for src in sources_to_process:
                dest_tbl = args.table if (args.table and len(sources_to_process) == 1) else f"{args.prefix}{sanitize_identifier(src)}"
                schema = adapter.get_schema(src)
                row_count = adapter.get_row_count(src)
                col_names = [c for c, _ in schema]

                if not args.quiet:
                    cnt_str = f" (~{row_count:,} rows)" if row_count is not None else ""
                    log_info(f"Importing '{src}' -> '{args.schema}.{dest_tbl}' ({len(schema)} columns){cnt_str}...")

                # Prepare destination table
                prepare_table(
                    cur,
                    sql_module,
                    args.schema,
                    dest_tbl,
                    schema,
                    mode=args.mode,
                    drop_cascade=not args.no_cascade,
                )
                conn.commit()

                # Stream copy
                row_chunks = adapter.stream_rows(src, chunk_size=args.chunk_size)
                rows_done = copy_stream(
                    conn,
                    cur,
                    sql_module,
                    args.schema,
                    dest_tbl,
                    col_names,
                    row_chunks,
                    total_expected=row_count,
                    quiet=args.quiet,
                )
                total_imported_all += rows_done

    except Exception as e:
        conn.rollback()
        log_error(f"Import failed: {e}")
        adapter.close()
        conn.close()
        sys.exit(1)

    adapter.close()
    conn.close()

    total_elapsed = time.time() - start_total_time
    if not args.quiet:
        log_success(
            f"Finished importing {len(sources_to_process)} table(s) ({total_imported_all:,} total rows) in {total_elapsed:.2f}s."
        )


if __name__ == "__main__":
    main()
