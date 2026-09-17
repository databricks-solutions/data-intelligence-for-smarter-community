"""Dual-mode configuration: works locally (CLI profile) and in Databricks Apps (service principal)."""
import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

CATALOG = os.environ.get("CATALOG", "main")
SCHEMA = os.environ.get("SCHEMA", "silver")


def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
    return WorkspaceClient(profile=profile)


def get_workspace_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    client = get_workspace_client()
    return client.config.host


def get_warehouse_id() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. Lab 06 sets it in app.yaml at deploy time; "
            "for local dev, export it to a SQL warehouse id you can use."
        )
    return wid


def get_oauth_token() -> str:
    """Return a bearer token string usable for SQL statements API.

    Note: w.config.token is often an empty string for OAuth/CLI auth types;
    w.config.authenticate() does the right thing and returns a fresh Bearer.
    """
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    if client.config.token:
        return client.config.token
    raise RuntimeError("Could not acquire Databricks OAuth token")


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"
