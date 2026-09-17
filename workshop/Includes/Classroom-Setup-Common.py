# Databricks notebook source
# MAGIC %pip install -qqq databricks-sdk==0.125.0
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Classroom-Setup-Common
# MAGIC Shared helpers for the **UBC Smart Community** workshop. Every per-lab
# MAGIC setup notebook (`Classroom-Setup-01` … `Classroom-Setup-06`) runs this file first
# MAGIC via `%run ./Classroom-Setup-Common`.
# MAGIC
# MAGIC Adapted from the Databricks Academy enablement-lab pattern. Isolation is per-user:
# MAGIC each student gets a `labuser_<username>` catalog with `bronze` / `silver` / `gold`
# MAGIC schemas. The app deployed in Lab 6 reads the **silver** schema.

# COMMAND ----------

# DBTITLE 1,Workshop configuration
# Course-wide config. Change here, not in the lab notebooks.
CATALOG_PREFIX = "labuser"                 # per-user catalog is <prefix>_<username>
BRONZE_SCHEMA = "bronze"                     # medallion schema names (parameterized so
SILVER_SCHEMA = "silver"                     # a shared-workspace validation run can point
GOLD_SCHEMA = "gold"                         # them elsewhere without editing every notebook)
SCHEMAS = [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]
APP_SCHEMA = SILVER_SCHEMA                    # schema the Lab 6 app points at

# The 11 tables the single-tab app depends on (produced across labs 01–04).
APP_TABLES = [
    "dim_buildings", "dim_zones",
    "sc_access_events", "sc_device_health",
    "mobility_presence",
    "wireless_cells", "wireless_connectivity", "wireless_node_health",
    "connectivity_presence", "sc_gateway_network",
    "udl_campus_metrics",
]

# Deterministic seed so every student's data matches the reference build.
SEED = 42

# Workshop scale: fraction of the full reference row counts, so per-student
# generation finishes in a couple of minutes on Serverless. Lab 1 reads this.
# Set to 1.0 to reproduce the full reference dataset.
WORKSHOP_SCALE = 0.15

# COMMAND ----------

## Determines if in Vocareum or Other Workspace and sets up the catalog
## Usage: my_catalog = build_user_catalog() within your demo/lab setup.

import re
from typing import Optional

def _safe_uc_name(value: str) -> str:
    # UC identifiers are generally safest with letters, numbers, underscores
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "user"


def _current_user_email() -> str:
    """
    Get the user's name and email address.
    """
    return spark.sql("SELECT current_user()").first()[0]


def _get_workspace_catalogs():
    """
    Returns a set of Catalogs visible to that user.
    """
    list_of_catalogs_in_workspace = [row["catalog"].strip().lower() for row in spark.sql("SHOW CATALOGS").collect()]
    return list_of_catalogs_in_workspace


def _catalog_exists(name: str, catalogs) -> bool:
    """
    Catalog checker to see if the catalog already exists for that user.
    """
    catalog_exists = name.lower() in catalogs
    return catalog_exists


def build_user_catalog(prefix: str = CATALOG_PREFIX, catalog_forced=None) -> str:
    """
    Returns a UC catalog name for the current user.

    Vocareum behavior:
      - If a catalog equals the user's 'labuserxxx' name and already exists,
        assume you are in Vocareum and use it.

    Other workspaces:
      - Use <prefix>_<user> and create it if possible for that user.
    """
    user_email = _current_user_email()
    user_name = user_email.split("@")[0]
    safe_user_name = _safe_uc_name(user_name)

    # VOCAREUM CHECKER: catalog is just the username (already provisioned)
    vocareum_catalog_name = safe_user_name

    if user_email.lower().endswith("@vocareum.com"):
        print("✅ Vocareum workspace detected.")
        if _catalog_exists(name=vocareum_catalog_name, catalogs=_get_workspace_catalogs()):
            print(f"✅ Using existing Vocareum catalog: '{vocareum_catalog_name}'.")
            return vocareum_catalog_name
        else:
            raise ValueError(
                f"❌ Catalog '{vocareum_catalog_name}' does not exist in this Vocareum workspace. "
                "Please create the catalog or verify the catalog name before continuing."
            )

    # OTHER WORKSPACE SETUP
    else:
        print("ℹ️ Non-Vocareum workspace detected. Setting up catalog.")
        # Limit to 19 chars so catalog.schema.table stays within the 64-char limit.
        safe_user_name_char_restrict = safe_user_name[:19]

        if catalog_forced is None:
            catalog_name = f"{prefix}_{safe_user_name_char_restrict}"
            print(f"ℹ️ Using default catalog name: '{catalog_name}'.")
        else:
            catalog_name = catalog_forced
            print(f"ℹ️ Using specified catalog name: '{catalog_name}'.")

        if _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) is True:
            print(f"✅ Catalog '{catalog_name}' already exists. Using this catalog.")
            return catalog_name
        elif _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) is False and catalog_forced is not None:
            raise RuntimeError(
                f"❌ Catalog '{catalog_name}' does not exist in this workspace. "
                "A forced catalog name must reference an existing catalog."
            )
        else:
            try:
                print(f"ℹ️ Catalog '{catalog_name}' not found. Creating it now...")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
                print(f"✅ Catalog '{catalog_name}' created successfully.")
                return catalog_name
            except Exception as e:
                print(
                    f"⚠️ Could not create catalog '{catalog_name}'. "
                    "You may not have privileges to create catalogs in this workspace.\n"
                    f"Error: {e}"
                )


# COMMAND ----------

def create_schemas(in_catalog: str, schemas_to_create: list):
    """
    Create one or more schemas in a Unity Catalog catalog (idempotent).
    Example: create_schemas("labuser_jane", ["bronze", "silver", "gold"])
    """
    print(f"\n{'='*60}")
    print(f"  STEP 1: Verifying catalog exists: {in_catalog}")
    print(f"{'='*60}")
    try:
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        if in_catalog not in catalogs:
            raise ValueError(
                f"Catalog '{in_catalog}' does not exist.\n"
                f"  Available catalogs: {', '.join(catalogs)}"
            )
        print(f"  Catalog '{in_catalog}' exists.")
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"  Failed to verify catalog: {e}")

    print(f"\n{'='*60}")
    print(f"  STEP 2: Setting up {len(schemas_to_create)} schema(s) in catalog: {in_catalog}")
    print(f"{'='*60}")

    existing_schemas = set(
        row.databaseName for row in spark.sql(f"SHOW SCHEMAS IN `{in_catalog}`").collect()
    )

    created = 0
    for i, schema_name in enumerate(schemas_to_create, start=1):
        full_name = f"`{in_catalog}`.`{schema_name}`"
        print(f"  [{i}/{len(schemas_to_create)}] Checking: {full_name}...", end=" ")
        if schema_name in existing_schemas:
            print("ALREADY EXISTS")
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_name}")
            created += 1
            print("CREATED")

    print(f"\n{'='*60}")
    print(f"  COMPLETE: {created} schema(s) created, {len(schemas_to_create) - created} already existed.")
    print(f"{'='*60}\n")


# COMMAND ----------

# -----------------------------------------------
# CHECK COMPUTE FUNCTION
# compute_validation(recommended_serverless_version=..., recommend_dbr_classic_version=...)
# Warns (does not fail) if the user is on a different compute type/version than tested.
# -----------------------------------------------

import os


def _get_env():
    is_serverless = os.environ.get("IS_SERVERLESS", "FALSE").upper()
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")

    if is_serverless == "TRUE":
        current_serverless_version = int(runtime_version.split(".")[1])
    else:
        current_serverless_version = None

    if is_serverless == "FALSE":
        try:
            current_dbr_version_all_purpose = float(runtime_version)
        except ValueError:
            current_dbr_version_all_purpose = None
    else:
        current_dbr_version_all_purpose = None

    return {
        "is_serverless": is_serverless,
        "current_serverless_version": current_serverless_version,
        "current_dbr_version_all_purpose": current_dbr_version_all_purpose,
    }


def _render_result(compute_type, recommended, current, match):
    if match:
        badge = '<span style="color:#2e7d32;font-weight:600">&#10003; Match</span>'
        detail = f"Version {current}"
        row_style = ""
    else:
        badge = '<span style="color:#c62828;font-weight:700">&#9888; Mismatch</span>'
        detail = f"Found {current} &mdash; recommended <strong>{recommended}</strong>"
        row_style = 'background:#FDE0DC;'
    return f"""
    <tr style="{row_style}">
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{compute_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{badge}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{detail}</td>
    </tr>"""


def _render_wrong_compute(expected_type, recommended):
    return f"""
    <tr style="background:#FDE0DC;">
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{expected_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0"><span style="color:#c62828;font-weight:700">&#9888; Wrong compute type</span></td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">This notebook expects <strong>{expected_type}</strong> (version {recommended})</td>
    </tr>"""


def _has_mismatch(rows_html):
    return "#FDE0DC" in rows_html


def _display(title, rows):
    joined = "".join(rows)
    header_bg = "#FF5F46" if _has_mismatch(joined) else "#1b3a4b"
    html = f"""
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:{header_bg};color:#fff;padding:10px 16px;font-size:16px;font-weight:600">{title}</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Compute</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Status</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Details</th>
        </tr>
        {joined}
      </table>
    </div>"""
    displayHTML(html)


def compute_validation(recommended_serverless_version: int = None, recommend_dbr_classic_version: float = None):
    """Warn (not fail) when the user is not on the tested compute type/version."""
    if recommended_serverless_version is None and recommend_dbr_classic_version is None:
        raise ValueError("Specify a compute type to check.")

    env_values = _get_env()
    csv = env_values["current_serverless_version"]
    cdv = env_values["current_dbr_version_all_purpose"]

    rows = []
    if recommended_serverless_version is not None and recommend_dbr_classic_version is None:
        if csv is None:
            rows.append(_render_wrong_compute("Serverless", recommended_serverless_version))
        else:
            rows.append(_render_result("Serverless", recommended_serverless_version, csv, csv == recommended_serverless_version))
        _display(f"Compute Check &mdash; Tested on Serverless v{recommended_serverless_version}", rows)
    elif recommended_serverless_version is None and recommend_dbr_classic_version is not None:
        if cdv is None:
            rows.append(_render_wrong_compute("All-Purpose", recommend_dbr_classic_version))
        else:
            rows.append(_render_result("All-Purpose", recommend_dbr_classic_version, cdv, cdv == recommend_dbr_classic_version))
        _display(f"Compute Check &mdash; Tested on All-Purpose DBR {recommend_dbr_classic_version}", rows)
    else:
        if cdv is not None:
            rows.append(_render_result("All-Purpose", recommend_dbr_classic_version, cdv, cdv == recommend_dbr_classic_version))
        if csv is not None:
            rows.append(_render_result("Serverless", recommended_serverless_version, csv, csv == recommended_serverless_version))
        _display(f"Compute Check &mdash; Tested on All-Purpose DBR {recommend_dbr_classic_version} / Serverless v{recommended_serverless_version}", rows)


# COMMAND ----------

def setup_complete_msg():
    print('\n------------------------------------------------------------------------------')
    print('✅ SETUP COMPLETE!')
    print('------------------------------------------------------------------------------')

# COMMAND ----------

def display_config_values(config_values, copy_values=False):
    """Displays list of (key, value[, copy_value]) tuples as a styled HTML table."""
    rows = ""
    for name, value, *rest in config_values:
        copy_value = rest[0] if rest else (copy_values or "")
        copy_btn = ""
        if copy_value:
            if isinstance(copy_value, str):
                copy_value = f'data-copy="{copy_value}"'
            else:
                copy_value = ""
            copy_btn = """
                <button type="button" onclick="copyFromSibling(this)"
                    style="margin-left:auto;padding:2px 10px;border:1px solid #ccc;border-radius:4px;background:#f5f5f5;cursor:pointer;font-size:13px;flex:0 0 auto;min-width:68px; text-align:center; box-sizing:border-box;">
                    Copy
                </button>"""
        rows += f"""
        <tr>
          <td style="padding:6px 12px;white-space:nowrap;border-bottom:1px solid #e0e0e0;font-weight:600">{name}:</td>
          <td style="padding:6px 12px;border-bottom:1px solid #e0e0e0">
            <div style="display:flex; align-items:center; gap:12px;">
                <span class="copy-source" {copy_value} style="display:inline-block;padding:4px 8px;font-size:15px;min-width:0;">{value}</span>
                {copy_btn}
            </div>
          </td>
        </tr>"""

    html = """
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:#1b3a4b;color:#fff;padding:10px 16px;font-size:16px;font-weight:600">Configuration Values</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Information</th>
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Value</th>
        </tr>""" + rows + """
      </table>
    </div>
    <script>
        if (!window.copyFromSibling) {
            window.copyFromSibling = function(btn) {
            var source = btn.parentElement.querySelector('.copy-source');
            if (!source) return;
            var text = source.dataset.copy || source.innerText;
            var t = document.createElement('textarea');
            t.value = text; t.style.position = 'fixed'; t.style.opacity = '0'; t.style.pointerEvents = 'none';
            document.body.appendChild(t); t.select(); document.execCommand('copy'); document.body.removeChild(t);
            var original = btn.textContent; btn.textContent = 'Copied!';
            setTimeout(function() { btn.textContent = original; }, 1500);
            };
        }
    </script>"""
    displayHTML(html)

# COMMAND ----------

import hashlib
from databricks.sdk import WorkspaceClient

workspace = WorkspaceClient()
my_username = workspace.current_user.me().user_name


def unique_name(notebook_scope: bool):
    name = my_username.split("@")[0]
    if notebook_scope:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().removeprefix(
            f'/Users/{my_username}/'
        )
        name += f'-{hashlib.sha1(notebook_path.encode("utf-8")).hexdigest()[:6]}'
    return name.lower()

# COMMAND ----------

# DBTITLE 1,App management
from databricks.sdk.service.apps import App
from databricks.sdk.errors import AlreadyExists, BadRequest, NotFound


def app_name(notebook_scope: bool):
    name = unique_name(notebook_scope)
    name = re.sub(r'[^a-z0-9]+', '-', name)
    name = re.sub(r'-+', '-', name)
    name = name.strip('-')
    return name[-30:]


def app_create(notebook_scope=True):
    name = app_name(notebook_scope)
    try:
        workspace.apps.create(app=App(name=name), no_compute=True)
        workspace.apps.wait_get_app_stopped(name=name)
        print(f"✅ Created App '{name}'.")
    except AlreadyExists:
        print(f"✅ App check. App '{name}' already exists.")
    sp = workspace.apps.get(name=name).service_principal_client_id
    try:
        workspace.apps.start(name=name)
    except BadRequest:
        pass
    return name, sp


def app_cleanup(name: str):
    try:
        workspace.apps.delete(name=name)
        print(f"✅ Deleted app '{name}'.")
    except NotFound:
        print(f"ℹ️ App '{name}' not found; nothing to delete.")
    except BadRequest:
        print(f"⚠️ Unable to delete app '{name}'; please try again in a moment.")

# COMMAND ----------

import shutil
from pathlib import Path


def home(relative_path: str = "") -> str:
    return str(Path(f"/Workspace/Users/{my_username}") / relative_path)


def copy_folder(src: str, dst: str, clean: bool = False) -> str:
    dst_path = Path(dst)
    if clean and dst_path.exists():
        for child in dst_path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    dst_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, str(dst_path), dirs_exist_ok=True)
    return str(dst_path)

# COMMAND ----------

# DBTITLE 1,Workshop data provisioning (telemetry generation + geodata volume)
# The real UBC open geodata files uploaded to a Volume for students to ingest in Lab 02.
GEODATA_VOLUME = "raw_geodata"
GEODATA_FILES = ["ubcv_buildings.geojson", "ubcv_neighbourhoods.geojson"]


def _course_includes_dir() -> str:
    """Absolute /Workspace path to the course Includes folder.

    Inside a %run include, notebookPath() returns the CALLING lab notebook's path
    (the workshop root), so Includes sits alongside it.
    """
    lab_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return f"/Workspace{os.path.dirname(lab_path)}/Includes"


def ensure_bronze_data(catalog: str):
    """Idempotently provision the workshop's raw inputs:

    1. Generate the 9 synthetic *telemetry* Bronze tables into `<catalog>.bronze`
       (skipped if already present — so only the first lab pays the cost).
    2. Upload the real UBC open geodata (UBCGeodata, CC BY 4.0) to the Volume
       `<catalog>.bronze.raw_geodata` so students ingest it in Lab 02.

    Dimension tables (dim_buildings, dim_zones) are NOT created here — those come
    from the geodata in Lab 02.
    """
    import sys
    includes = _course_includes_dir()

    # 1) Synthetic telemetry Bronze tables (one-time).
    if spark.catalog.tableExists(f"{catalog}.{BRONZE_SCHEMA}.sc_access_events"):
        print("✅ Bronze telemetry already present — skipping generation.")
    else:
        if includes not in sys.path:
            sys.path.insert(0, includes)
        from smart_campus_generators import generate_telemetry
        print("Generating synthetic telemetry Bronze tables (one-time, ~2 min)...")
        generate_telemetry(spark, catalog, BRONZE_SCHEMA, scale=WORKSHOP_SCALE, seed=SEED,
                           geojson_dir=f"{includes}/geojson")

    # 2) Real UBC open geodata → Volume (one-time).
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{BRONZE_SCHEMA}`.`{GEODATA_VOLUME}`")
    vol_dir = f"/Volumes/{catalog}/{BRONZE_SCHEMA}/{GEODATA_VOLUME}"
    for fn in GEODATA_FILES:
        dst = f"{vol_dir}/{fn}"
        if not os.path.exists(dst):
            shutil.copy(f"{includes}/geojson/{fn}", dst)
    print(f"✅ UBC open geodata uploaded to Volume: {vol_dir}")
    return vol_dir
