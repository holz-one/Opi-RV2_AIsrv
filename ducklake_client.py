#!/usr/bin/env python3
"""
DuckLake & Warehouse Python Client SDK
======================================
A lightweight, zero-dependency Python client for interacting with the Orange Pi RV2
DuckLake Analytical Lakehouse and Warehouse Server.

Matches the official DuckLake Guide specifications:
- RESTful Table Operations (GET, PUT, PATCH, DELETE)
- Project Partition Management (/projects/<id>)
- Raw File Warehouse Storage (/warehouse/files)
- High-Speed In-Memory Analytical DuckDB Queries (/ducklake/query)
- Data Inlining Flush and Maintenance Operations
- Optional Pandas DataFrame support for seamless data science integration.

Usage Example:
--------------
    from ducklake_client import DuckLakeClient

    # Initialize client (optionally scoped to a project partition)
    client = DuckLakeClient(base_url="http://192.168.18.15:5000", project="dawahtools")

    # 1. Execute analytical SQL queries
    res = client.query("SELECT * FROM cluster_nodes LIMIT 5")
    print(res.columns)
    for row in res.rows:
        print(row)

    # Convert directly to Pandas DataFrame if pandas is installed:
    df = res.to_df()

    # 2. Append structured records (auto data-inlining or Parquet)
    client.write_table("metrics", [
        {"metric": "cpu_load", "value": 0.42, "unit": "pct"},
        {"metric": "ram_usage", "value": 1.25, "unit": "GB"}
    ], mode="append")

    # 3. Read table data and schema
    table_data = client.read_table("metrics", limit=100)
    print(table_data["schema"])

    # 4. Upload raw file to Warehouse
    client.upload_warehouse_file("raw_logs.json", "path/to/local_log.json")

    # 5. Flush inlined data buffer into Parquet
    client.flush_inlined()
"""

import json
import os
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Union


class DuckLakeQueryResult:
    """Encapsulates the tabular result of a DuckLake SQL query."""

    def __init__(self, data: Dict[str, Any]):
        self.raw = data
        self.status = data.get("status", "unknown")
        self.columns: List[str] = data.get("columns", [])
        self.types: List[str] = data.get("types", [])
        self.rows: List[List[Any]] = data.get("rows", [])
        self.row_count: int = data.get("row_count", len(self.rows))
        self.execution_time_ms: float = data.get("execution_time_ms", 0.0)
        self.error: Optional[str] = data.get("error")

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Converts query rows into a list of column-mapped dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def to_df(self):
        """
        Converts query rows into a pandas DataFrame.
        Requires `pandas` to be installed in the runtime environment.
        """
        try:
            import pandas as pd
            return pd.DataFrame(self.rows, columns=self.columns)
        except ImportError:
            raise ImportError(
                "pandas is required for to_df(). Install it via `pip install pandas` "
                "or use res.to_dict_list() / res.rows."
            )

    def __repr__(self) -> str:
        if self.status != "success":
            return f"<DuckLakeQueryResult error='{self.error}'>"
        return f"<DuckLakeQueryResult rows={self.row_count} cols={len(self.columns)} time={self.execution_time_ms}ms>"


class DuckLakeClient:
    """Client interface for DuckLake analytical storage and Warehouse file management."""

    def __init__(self, base_url: str = "http://127.0.0.1:5000", project: Optional[str] = None, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.project = project.strip() if project else None
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _http_request(
        self,
        method: str,
        path: str,
        data: Optional[Union[bytes, str]] = None,
        json_data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Performs HTTP request using Python's standard library urllib."""
        url = self._url(path)
        if params:
            query_str = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{url}?{query_str}"

        req_headers = headers or {}
        body_bytes = None

        if json_data is not None:
            body_bytes = json.dumps(json_data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        elif data is not None:
            if isinstance(data, str):
                body_bytes = data.encode("utf-8")
            else:
                body_bytes = data

        req = urllib.request.Request(url, data=body_bytes, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp_bytes = resp.read()
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    return json.loads(resp_bytes.decode("utf-8"))
                try:
                    return json.loads(resp_bytes.decode("utf-8"))
                except Exception:
                    return {"status": "success", "raw_content": resp_bytes}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            try:
                err_json = json.loads(err_body)
                return err_json
            except Exception:
                return {"status": "error", "error": f"HTTP {e.code}: {e.reason}", "detail": err_body}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ==========================================================================
    # 1. System Health & Catalog Discovery
    # ==========================================================================
    def get_status(self) -> Dict[str, Any]:
        """Retrieves server status, memory metrics, and DuckDB health."""
        return self._http_request("GET", "/status")

    def get_manifest(self) -> Dict[str, Any]:
        """Retrieves the self-describing DuckLake service manifest and active catalog."""
        return self._http_request("GET", "/manifest")

    def get_catalog(self) -> Dict[str, Any]:
        """Returns catalog of registered Parquet tables and warehouse files."""
        return self._http_request("GET", "/ducklake/catalog")

    # ==========================================================================
    # 2. Analytical SQL Queries (DuckDB Compute Layer)
    # ==========================================================================
    def query(self, sql_query: str) -> DuckLakeQueryResult:
        """
        Executes an analytical SQL query on the DuckDB compute engine.
        Returns a DuckLakeQueryResult object.
        """
        res = self._http_request("POST", "/ducklake/query", json_data={"query": sql_query})
        return DuckLakeQueryResult(res)

    def execute(self, sql_query: str) -> DuckLakeQueryResult:
        """Alias for query()."""
        return self.query(sql_query)

    # ==========================================================================
    # 3. Project Partition Management
    # ==========================================================================
    def list_projects(self) -> List[Dict[str, Any]]:
        """Lists all project partitions and storage metrics."""
        res = self._http_request("GET", "/projects")
        return res.get("projects", [])

    def create_project(self, project_id: str, display_name: Optional[str] = None, description: str = "") -> Dict[str, Any]:
        """Creates a new isolated project partition under /srv/DuckLake and /srv/Warehouse."""
        payload = {
            "project_id": project_id,
            "display_name": display_name or project_id,
            "description": description,
        }
        return self._http_request("POST", "/projects", json_data=payload)

    def get_project_details(self, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves details, tables, and files for a project partition."""
        pid = project_id or self.project or "default"
        return self._http_request("GET", f"/projects/{pid}")

    def delete_project(self, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Deletes a project partition and drops all its tables and files."""
        pid = project_id or self.project
        if not pid or pid == "default":
            raise ValueError("Cannot delete default project or empty project_id")
        return self._http_request("DELETE", f"/projects/{pid}")

    # ==========================================================================
    # 4. DuckLake Parquet Table Operations (RESTful)
    # ==========================================================================
    def write_table(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        mode: str = "append",
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Writes JSON records to a DuckLake table.
        Uses PUT (mode="replace") or PATCH (mode="append").
        Small inserts automatically leverage DuckLake's Data Inlining feature.
        """
        pid = project_id or self.project
        if pid and pid != "default":
            path = f"/projects/{pid}/tables/{table_name}"
        else:
            path = f"/ducklake/tables/{table_name}"

        method = "PATCH" if mode.lower() == "append" else "PUT"
        return self._http_request(method, path, json_data=records)

    def upload_table_file(
        self,
        table_name: str,
        file_path_or_buffer: Union[str, bytes],
        mode: str = "replace",
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Uploads a local CSV, TSV, JSON, or Parquet file directly to create or replace a DuckLake table.
        """
        pid = project_id or self.project
        if pid and pid != "default":
            path = f"/projects/{pid}/tables/{table_name}"
        else:
            path = f"/ducklake/tables/{table_name}"

        if isinstance(file_path_or_buffer, str):
            with open(file_path_or_buffer, "rb") as f:
                content = f.read()
            ext = os.path.splitext(file_path_or_buffer)[1].lower()
        else:
            content = file_path_or_buffer
            ext = ".parquet"

        content_types = {
            ".csv": "text/csv",
            ".tsv": "text/tab-separated-values",
            ".json": "application/json",
            ".parquet": "application/octet-stream",
            ".pq": "application/octet-stream",
        }
        ct = content_types.get(ext, "application/octet-stream")
        method = "PATCH" if mode.lower() == "append" else "PUT"
        return self._http_request(method, path, data=content, headers={"Content-Type": ct})

    def read_table(
        self,
        table_name: str,
        limit: int = 50,
        offset: int = 0,
        columns: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reads schema, statistics, and preview records for a DuckLake table."""
        pid = project_id or self.project
        path = f"/ducklake/tables/{table_name}"
        params = {"limit": limit, "offset": offset, "columns": columns, "project_id": pid}
        return self._http_request("GET", path, params=params)

    def delete_table(self, table_name: str, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Deletes a DuckLake table, removes underlying Parquet files, and cleans metadata."""
        pid = project_id or self.project
        path = f"/ducklake/tables/{table_name}"
        params = {"project_id": pid} if pid else None
        return self._http_request("DELETE", path, params=params)

    # ==========================================================================
    # 5. Raw Warehouse File Operations
    # ==========================================================================
    def upload_warehouse_file(
        self,
        remote_filename: str,
        file_path_or_data: Union[str, bytes],
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Uploads or replaces a raw file in the Warehouse partition.
        Accepts a local file path or raw bytes.
        """
        pid = project_id or self.project
        if pid and pid != "default":
            path = f"/projects/{pid}/files/{remote_filename}"
        else:
            path = f"/warehouse/files/{remote_filename}"

        if isinstance(file_path_or_data, str) and os.path.isfile(file_path_or_data):
            with open(file_path_or_data, "rb") as f:
                data = f.read()
        elif isinstance(file_path_or_data, str):
            data = file_path_or_data.encode("utf-8")
        else:
            data = file_path_or_data

        return self._http_request("PUT", path, data=data, headers={"Content-Type": "application/octet-stream"})

    def read_warehouse_file(
        self,
        remote_filename: str,
        preview: bool = False,
        max_lines: int = 100,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Downloads or previews a raw file from the Warehouse."""
        pid = project_id or self.project
        if pid and pid != "default":
            path = f"/warehouse/files/{pid}/{remote_filename}"
        else:
            path = f"/warehouse/files/{remote_filename}"
        params = {"preview": "true" if preview else None, "max_lines": max_lines}
        return self._http_request("GET", path, params=params)

    def delete_warehouse_file(self, remote_filename: str, project_id: Optional[str] = None) -> Dict[str, Any]:
        """Deletes a raw file from the Warehouse."""
        pid = project_id or self.project
        if pid and pid != "default":
            path = f"/projects/{pid}/files/{remote_filename}"
        else:
            path = f"/warehouse/files/{remote_filename}"
        return self._http_request("DELETE", path)

    # ==========================================================================
    # 6. DuckLake Guide Specifications & Maintenance Operations
    # ==========================================================================
    def flush_inlined(self, table_name: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Flushes inlined small-record buffers from the PostgreSQL catalog into persistent Parquet files.
        (Matches Chapter 3: Data Inlining maintenance CALL ducklake_flush_inlined_data)
        """
        pid = project_id or self.project
        path = f"/projects/{pid}/flush" if (pid and pid != "default") else "/ducklake/flush"
        return self._http_request("POST", path, json_data={"table_name": table_name})

    def compact(self, table_name: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Merges adjacent small Parquet files into consolidated files while preserving snapshot history.
        (Matches Chapter 5: Compaction CALL ducklake_merge_adjacent_files)
        """
        pid = project_id or self.project
        path = f"/projects/{pid}/compact" if (pid and pid != "default") else "/ducklake/compact"
        return self._http_request("POST", path, json_data={"table_name": table_name})

    def cleanup_old_files(self) -> Dict[str, Any]:
        """
        Physically removes unreferenced/expired Parquet files from disk.
        (Matches Chapter 5: CALL ducklake_cleanup_old_files)
        """
        return self._http_request("POST", "/ducklake/cleanup")

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """
        Retrieves snapshot history and change operations.
        (Matches Chapter 4: FROM snapshots())
        """
        res = self._http_request("GET", "/ducklake/snapshots")
        return res.get("snapshots", [])

    def sync(self) -> Dict[str, Any]:
        """Forces rescanning and catalog synchronization of DuckLake and Warehouse storage."""
        return self._http_request("POST", "/ducklake/sync")


# Convenience CLI entry point when executed directly
if __name__ == "__main__":
    import sys

    print("DuckLake & Warehouse Python Client SDK (v1.3.0)")
    host = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
    client = DuckLakeClient(base_url=host)
    print(f"Connecting to {host}...")
    status = client.get_status()
    print("Server Status:", status.get("status"))
    print("Storage Stats:", status.get("storage"))
