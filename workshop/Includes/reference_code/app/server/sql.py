"""Databricks SQL execution wrapper using databricks-sql-connector."""
from __future__ import annotations
import os
from typing import Any, List, Dict
from databricks import sql as dbsql
from .config import (
    get_workspace_host,
    get_warehouse_id,
    get_oauth_token,
    IS_DATABRICKS_APP,
)


def _server_hostname() -> str:
    host = get_workspace_host()
    # Strip scheme
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _http_path() -> str:
    return f"/sql/1.0/warehouses/{get_warehouse_id()}"


def run_query(query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Execute a SQL query via the serverless warehouse. Returns list of dicts.

    Uses positional parameters via connector's :name binding if params provided.
    """
    hostname = _server_hostname()
    http_path = _http_path()
    token = get_oauth_token()

    conn_args = {
        "server_hostname": hostname,
        "http_path": http_path,
        "access_token": token,
    }

    with dbsql.connect(**conn_args) as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, parameters=params)
            else:
                cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, r)) for r in rows]
