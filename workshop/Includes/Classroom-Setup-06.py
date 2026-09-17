# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# DBTITLE 1,Ensure catalog + schemas, create an empty App, install the reference app code
catalog = build_user_catalog()
create_schemas(catalog, SCHEMAS)
spark.sql(f"USE CATALOG `{catalog}`")

# Idempotent — no-op if the earlier labs already provisioned the data.
ensure_bronze_data(catalog)

DA_CATALOG = catalog
DA_SILVER = f"{catalog}.{SILVER_SCHEMA}"
DA_GOLD = f"{catalog}.{GOLD_SCHEMA}"

# Create an empty Databricks App (compute pre-warmed) to deploy into later.
my_app_name, my_app_sp = app_create(notebook_scope=True)

# Resolve the bundled reference app source (workshop/Includes/reference_code/app),
# which ships a pre-built frontend/dist so no npm build is needed. Workspace files
# are readable under /Workspace, so copy it into the student's home to edit + deploy.
# Inside a %run include, notebookPath() returns the CALLING lab notebook's path
# (the workshop root), so the reference code sits under <root>/Includes/reference_code.
_lab_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_ref_src = f"/Workspace{_lab_dir}/Includes/reference_code/app"
reference_app_path = copy_folder(src=_ref_src, dst=home("smart-campus-app"), clean=True)

# COMMAND ----------

display_config_values([
    ('Catalog', catalog, True),
    ('Silver schema (app reads this)', DA_SILVER, True),
    ('App name', f'<a href="/apps-v2/app/{my_app_name}/overview" target="_blank">{my_app_name}</a>', my_app_name),
    ('App service principal', my_app_sp, True),
    ('App source (your home)', f'<a href="#workspace{reference_app_path}" target="_blank">{reference_app_path}</a>', reference_app_path),
])
setup_complete_msg()
