# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 01</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lab 01 — Explore &amp; Profile the Data
# MAGIC
# MAGIC Before you transform anything, get to know your data. When you ran setup, your workspace
# MAGIC was seeded with a realistic **smart-community dataset** for the UBC Vancouver campus:
# MAGIC nine synthetic **telemetry** tables already landed in your **Bronze** schema, and the real
# MAGIC **UBC open geodata** (UBCGeodata, CC BY 4.0) was uploaded to a **Volume** for you to
# MAGIC ingest in Lab 02.
# MAGIC
# MAGIC In this lab you'll **query**, **profile**, and **check** that data in Unity Catalog — the
# MAGIC habits that keep a data project honest.
# MAGIC
# MAGIC ## Objectives
# MAGIC - List and inspect your **Bronze** tables in **Unity Catalog** (SQL + Catalog Explorer)
# MAGIC - **Profile** a table: row counts, distinct values, nulls, distributions
# MAGIC - Confirm the raw **UBCGeodata** file is staged in your **Volume**
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** What setup provisioned · **C.** Explore in Unity Catalog · **D.** Profile a table · **E.** Check the geodata Volume · **F.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border:1px solid #DCE0E2; border-radius:12px; overflow:hidden; margin:14px 0;">
# MAGIC   <div style="background:#FF3621; color:#fff; padding:10px 18px; font-weight:700; letter-spacing:0.04em; text-transform:uppercase; font-size:12pt;">Required: attach Serverless compute before running any cell</div>
# MAGIC   <div style="background:#F9F7F4; padding:14px 20px; color:#1B3139;">Click <strong>Connect</strong> (top-right) → <strong>Serverless</strong>. The setup cell provisions your catalog, generates the Bronze telemetry (one-time, ~2 min), and uploads the geodata to a Volume. Wait for <strong>SETUP COMPLETE</strong>.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. What setup provisioned
# MAGIC
# MAGIC - **Bronze telemetry** (9 tables) in `DA_BRONZE` — building access, device health, mobility,
# MAGIC   wireless (cells / coverage / KPIs), gateway connectivity, and daily campus metrics.
# MAGIC - **Raw geodata** in the Volume `GEODATA_DIR` — the real UBCGeodata building + neighbourhood
# MAGIC   files, waiting to be ingested in Lab 02.
# MAGIC
# MAGIC The dimension tables (`dim_buildings`, `dim_zones`) do **not** exist yet — you'll build them
# MAGIC from the geodata next lab. Let's confirm what's here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore in Unity Catalog
# MAGIC Everything lives under your catalog in **Unity Catalog**. You can browse it visually in
# MAGIC **Catalog Explorer** (left sidebar → Catalog → your `labuser_…` catalog → `bronze`), or
# MAGIC query the metadata directly.

# COMMAND ----------

# DBTITLE 1,List your Bronze tables
spark.sql(f"SHOW TABLES IN {DA_BRONZE}").display()

# COMMAND ----------

# DBTITLE 1,Inspect one table's schema
spark.sql(f"DESCRIBE TABLE {DA_BRONZE}.sc_access_events").display()

# COMMAND ----------

# DBTITLE 1,Row counts across the Bronze tables
tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {DA_BRONZE}").collect()]
counts = [(t, spark.table(f"{DA_BRONZE}.{t}").count()) for t in tables]
display(spark.createDataFrame(counts, ["table", "row_count"]).orderBy("table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Profile a table
# MAGIC Profiling = understanding shape and quality before you trust it. Let's profile
# MAGIC `sc_access_events` (a **security** signal) three ways.

# COMMAND ----------

# DBTITLE 1,Summary statistics (numeric + string columns)
spark.table(f"{DA_BRONZE}.sc_access_events").summary().display()

# COMMAND ----------

# DBTITLE 1,Categorical distribution — event types
spark.sql(f"""
    SELECT event_type, COUNT(*) AS n,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM {DA_BRONZE}.sc_access_events
    GROUP BY event_type
    ORDER BY n DESC
""").display()

# COMMAND ----------

# DBTITLE 1,Data-quality check — nulls in key columns
from pyspark.sql import functions as F

df = spark.table(f"{DA_BRONZE}.sc_access_events")
null_counts = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in ["bldg_uid", "event_ts", "occupant_id", "latitude"]
])
null_counts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Tip:** in the result grid above, click the **+ → Data Profile** tab to get an
# MAGIC > auto-generated visual profile (histograms, null %, distinct counts) with no code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Check the geodata Volume
# MAGIC The one piece of **real** data is the UBC open geodata. Setup uploaded it to a **Volume**
# MAGIC (governed file storage in Unity Catalog). Let's confirm it's there and peek inside — this
# MAGIC is the file you'll ingest in Lab 02.

# COMMAND ----------

# DBTITLE 1,List files in the geodata Volume
print("Geodata volume:", GEODATA_DIR)
display(dbutils.fs.ls(GEODATA_DIR))

# COMMAND ----------

# DBTITLE 1,Peek inside the buildings GeoJSON
import json

with open(f"{GEODATA_DIR}/ubcv_buildings.geojson") as f:
    gj = json.load(f)

feats = gj.get("features", [])
print(f"Building features: {len(feats)}")
print("First building properties:")
for k, v in list(feats[0]["properties"].items())[:8]:
    print(f"  {k}: {v}")
print("Geometry type:", feats[0]["geometry"]["type"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F. 🎯 Your turn
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC Profile a <strong>transit</strong> table instead. For <code>mobility_presence</code>:
# MAGIC find the number of <strong>distinct devices</strong> and the <strong>average dwell time</strong>, and check
# MAGIC whether <code>bldg_uid</code> is ever null. Which single building has the most records?
# MAGIC </div>

# COMMAND ----------

# TODO: profile {DA_BRONZE}.mobility_presence

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Recap
# MAGIC You explored and profiled the Bronze telemetry in Unity Catalog and confirmed the raw
# MAGIC **UBCGeodata** is staged in your Volume. In **Lab 02** you'll **ingest** that geodata into
# MAGIC `dim_buildings` and `dim_zones`, then clean everything into a trustworthy **Silver** layer.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
