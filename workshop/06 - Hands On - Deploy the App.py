# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 06</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 06 — Deploy the App
# MAGIC
# MAGIC The finale: a live **Databricks App** — an interactive UBC campus map over *your* data,
# MAGIC with a dropdown of dataset views and an inline **Ask Genie** panel. It's backed by your
# MAGIC **Silver** tables and the **Genie space** from Lab 05. This is your hackathon starting point.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Point the bundled app at **your** catalog, Silver schema, warehouse, and Genie space
# MAGIC - **Deploy** it to Databricks Apps and open the live URL
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** What you're deploying · **C.** Configure the app · **D.** Deploy · **E.** Open it · **F.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border-left:4px solid #FF3621; background:#FFF0EE; padding:12px 16px; border-radius:4px; margin:12px 0; color:#1B3139;">Attach <strong>Serverless</strong> and run setup. It creates an empty App for you and copies the app source into your Workspace home. Needs your Silver tables (Lab 02) and Genie Space ID (Lab 05).</div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. What you're deploying
# MAGIC A **FastAPI + React** app (source now in your home at the path shown above):
# MAGIC - `server/` queries your Silver tables through a SQL warehouse and calls the Genie API
# MAGIC - `frontend/dist/` is the pre-built campus-map UI (Leaflet) — no build step needed
# MAGIC - `app.yaml` holds the config the app reads at runtime (catalog, schema, warehouse, Genie space)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Configure the app
# MAGIC We rewrite `app.yaml` so the app reads *your* data. We pick a Serverless SQL warehouse in
# MAGIC this workspace, read back your Genie Space ID (saved in Lab 05), and template the file.

# COMMAND ----------

# DBTITLE 1,Find a SQL warehouse in this workspace
warehouses = list(workspace.warehouses.list())
serverless = [w for w in warehouses if getattr(w, "enable_serverless_compute", False)]
chosen = (serverless or warehouses)[0]
WAREHOUSE_ID = chosen.id
print(f"Using warehouse: {chosen.name} ({WAREHOUSE_ID})")

# COMMAND ----------

# DBTITLE 1,Read back the Genie Space ID saved in Lab 05
try:
    GENIE_SPACE_ID = spark.table(f"{DA_CATALOG}._workshop_config.genie").first()["genie_space_id"]
    print(f"Genie Space ID from Lab 05: {GENIE_SPACE_ID}")
except Exception:
    GENIE_SPACE_ID = ""
    print("⚠️ No saved Genie Space ID found. Complete Lab 05 section E, or paste it in the widget below.")

dbutils.widgets.text("genie_space_id_override", "", "Genie Space ID (optional override)")
if dbutils.widgets.get("genie_space_id_override").strip():
    GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id_override").strip()

if GENIE_SPACE_ID:
    print(f"✅ Ask Genie will be enabled — space {GENIE_SPACE_ID}")
else:
    print("⚠️ Ask Genie will be DISABLED — the map still works, but the Ask Genie panel will")
    print("   report 'not configured'. Finish Lab 05, or paste your Space ID in the widget above,")
    print("   then re-run this notebook to enable it.")

# COMMAND ----------

# DBTITLE 1,Template app.yaml with your config
import yaml

app_yaml_path = f"{reference_app_path}/app.yaml"
with open(app_yaml_path) as f:
    cfg = yaml.safe_load(f)

# Rewrite env values
env_map = {e["name"]: e for e in cfg.get("env", [])}
env_map["DATABRICKS_WAREHOUSE_ID"]["value"] = WAREHOUSE_ID
env_map["CATALOG"]["value"] = DA_CATALOG
env_map["SCHEMA"]["value"] = APP_SCHEMA           # the app reads your Silver detail tables
env_map["GENIE_SPACE_ID"]["value"] = GENIE_SPACE_ID
# Point the warehouse resource at the chosen warehouse too
for r in cfg.get("resources", []):
    if "sql_warehouse" in r:
        r["sql_warehouse"]["id"] = WAREHOUSE_ID

with open(app_yaml_path, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)

print("Wrote app.yaml:")
print(open(app_yaml_path).read())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #FFAB00; background:#FFF8E1; padding:12px 16px; border-radius:4px; margin:12px 0; color:#5D4037;">
# MAGIC <strong>Grant the app access.</strong> The app runs as its own service principal (shown in setup).
# MAGIC It needs <code>USE CATALOG</code> + <code>USE SCHEMA</code> + <code>SELECT</code> on your Silver schema,
# MAGIC <code>CAN_USE</code> on the warehouse, and <code>CAN_RUN</code> on your Genie space. The cell below grants
# MAGIC the UC privileges; grant the warehouse + Genie permissions in their UIs if the app can't reach them.
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Grant the app's service principal read access to your Silver schema
for stmt in [
    f"GRANT USE CATALOG ON CATALOG `{DA_CATALOG}` TO `{my_app_sp}`",
    f"GRANT USE SCHEMA ON SCHEMA `{DA_CATALOG}`.`{SILVER_SCHEMA}` TO `{my_app_sp}`",
    f"GRANT SELECT ON SCHEMA `{DA_CATALOG}`.`{SILVER_SCHEMA}` TO `{my_app_sp}`",
]:
    try:
        spark.sql(stmt)
        print("✅", stmt)
    except Exception as e:
        print("⚠️", stmt, "->", e)

# COMMAND ----------

# DBTITLE 1,Grant the app's service principal CAN_USE on the SQL warehouse
# Without this the app can create SQL sessions but every query fails with
# "Error during request to server". update_permissions is additive (it does NOT
# replace other users' access on a shared warehouse).
from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel

try:
    workspace.warehouses.update_permissions(
        id=WAREHOUSE_ID,
        access_control_list=[
            WarehouseAccessControlRequest(
                service_principal_name=my_app_sp,
                permission_level=WarehousePermissionLevel.CAN_USE,
            )
        ],
    )
    print(f"✅ Granted CAN_USE on warehouse {WAREHOUSE_ID} to the app service principal.")
except Exception as e:
    print(f"⚠️ Could not grant warehouse access automatically: {e}\n"
          f"   Grant CAN_USE on warehouse {WAREHOUSE_ID} to {my_app_sp} in the SQL Warehouses UI.")

# COMMAND ----------

# DBTITLE 1,Grant the app's service principal CAN_RUN on the Genie space
# The app runs as its own service principal; without CAN_RUN on the Genie space
# the "Ask Genie" panel fails with "Genie space not configured — contact admin".
if GENIE_SPACE_ID:
    try:
        workspace.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/genie/{GENIE_SPACE_ID}",
            body={"access_control_list": [
                {"service_principal_name": my_app_sp, "permission_level": "CAN_RUN"}
            ]},
        )
        print(f"✅ Granted CAN_RUN on Genie space {GENIE_SPACE_ID} to the app service principal.")
    except Exception as e:
        print(f"⚠️ Could not grant Genie access automatically: {e}\n"
              f"   Grant CAN_RUN on the Genie space to {my_app_sp} in the Genie UI.")
else:
    print("ℹ️ No Genie space id set — skipping Genie grant. (Complete Lab 05 first.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Deploy
# MAGIC Deploy the configured source to your app and wait for it to come up.

# COMMAND ----------

# DBTITLE 1,Deploy to Databricks Apps
from databricks.sdk.service.apps import AppDeployment
import time

# An app must be RUNNING before you can deploy to it. On a fresh workspace the
# compute cold-start can take a minute or two, so wait for ACTIVE (starting it if
# it's stopped) before deploying.
print("Waiting for the app compute to be ACTIVE ...")
app = workspace.apps.get(name=my_app_name)
for _ in range(60):
    state = str(app.compute_status.state if app.compute_status else "").upper()
    if "ACTIVE" in state:
        break
    if "STOPPED" in state or "ERROR" in state:
        try:
            workspace.apps.start(name=my_app_name)
        except Exception:
            pass
    time.sleep(10)
    app = workspace.apps.get(name=my_app_name)
print("App compute state:", app.compute_status.state if app.compute_status else "?")

deployment = workspace.apps.deploy_and_wait(
    app_name=my_app_name,
    app_deployment=AppDeployment(source_code_path=reference_app_path),
)
print(f"Deployment state: {deployment.status.state if deployment.status else deployment}")

# COMMAND ----------

# DBTITLE 1,Get the live URL
app = workspace.apps.get(name=my_app_name)
print("Compute status:", app.compute_status.state if app.compute_status else "?")
print("\n🎉 Your app is live at:\n", app.url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Open it
# MAGIC Click the URL above. You should see the **UBC campus map**; use the dropdown to switch
# MAGIC dataset views (Building Access, Wireless Presence, UBC Urban Data Lake, …) and the
# MAGIC **Ask Genie** panel on the right to ask questions of your data.
# MAGIC
# MAGIC > First load can take ~30–60s while the app warms up and the warehouse starts.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F. 🎯 Your turn — make it yours for the hackathon
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC The app source is in your home — edit and redeploy:
# MAGIC <ul style="margin:8px 0 0 0; padding-left:20px; line-height:1.8;">
# MAGIC   <li>Change the header title in <code>frontend/dist/…</code>? (Or rebuild from <code>frontend/src</code> in the full repo.)</li>
# MAGIC   <li>Add a new dataset view in <code>server/datasets.py</code> — e.g. a <strong>transit hub score</strong> from your Gold <code>building_activity</code> table.</li>
# MAGIC   <li>Point the app at <code>gold</code> instead of <code>silver</code> and expose your fused per-building metrics.</li>
# MAGIC </ul>
# MAGIC Re-run the deploy cell after any change.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ You did it
# MAGIC You took synthetic data from **generation → Bronze → Silver → Gold → AI/BI → Genie → a live
# MAGIC App**, entirely on Databricks. That's the full Data Intelligence Platform loop — and a
# MAGIC running head-start for your hackathon project. Good luck! 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
