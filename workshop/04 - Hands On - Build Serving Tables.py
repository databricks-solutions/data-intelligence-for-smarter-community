# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 04</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 04 — Build Serving Tables (Silver → Gold)
# MAGIC
# MAGIC **Gold** tables are shaped for consumption: pre-joined, pre-aggregated, one tidy row per
# MAGIC thing people ask about. They make dashboards fast, make **Genie** accurate (Lab 05), and
# MAGIC give your app clean summaries to serve.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Aggregate Silver into **per-building** and **per-cell** Gold tables
# MAGIC - Practice multi-table **joins** across the datasets
# MAGIC - Produce tables with clear names + column names that read well in natural language
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** Per-building activity (Gold) · **C.** Per-cell health (Gold) · **D.** Verify · **E.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border-left:4px solid #FF3621; background:#FFF0EE; padding:12px 16px; border-radius:4px; margin:12px 0; color:#1B3139;">Attach <strong>Serverless</strong> and run setup. Reads your <strong>Silver</strong> tables from Lab 02.</div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Per-building activity — `gold.building_activity`
# MAGIC
# MAGIC One row per building that fuses **security** (access events), **transit** (mobility), and
# MAGIC **campus** (UDL) signals with the building's map coordinates. This is the table an analyst
# MAGIC — or Genie — most wants to query.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DA_GOLD}.building_activity AS
WITH access AS (
  SELECT bldg_uid,
         ANY_VALUE(building_name) AS building_name,
         ANY_VALUE(category)      AS category,
         ANY_VALUE(latitude)      AS latitude,
         ANY_VALUE(longitude)     AS longitude,
         COUNT(*)                 AS access_events,
         SUM(CASE WHEN event_type = 'door_forced' THEN 1 ELSE 0 END) AS forced_doors,
         COUNT(DISTINCT occupant_id) AS unique_occupants
  FROM {DA_SILVER}.sc_access_events
  GROUP BY bldg_uid
),
mobility AS (
  SELECT bldg_uid,
         COUNT(DISTINCT anon_device_id) AS unique_devices,
         AVG(dwell_seconds)             AS avg_dwell_seconds
  FROM {DA_SILVER}.mobility_presence
  GROUP BY bldg_uid
),
udl AS (
  SELECT bldg_uid,
         AVG(occupancy_pct)          AS avg_occupancy_pct,
         SUM(safety_incidents_count) AS total_incidents,
         AVG(energy_consumption_kwh) AS avg_energy_kwh
  FROM {DA_SILVER}.udl_campus_metrics
  GROUP BY bldg_uid
)
-- Every telemetry dataset shares the same building key, `bldg_uid`, and carries
-- its own name/category/coordinates, so we key the fused table on bldg_uid and
-- use the access stream as the spine (it covers all campus buildings).
SELECT
  a.bldg_uid,
  a.building_name,
  a.category,
  a.latitude,
  a.longitude,
  a.access_events,
  a.forced_doors,
  a.unique_occupants,
  COALESCE(m.unique_devices, 0)  AS unique_devices,
  ROUND(m.avg_dwell_seconds, 1)  AS avg_dwell_seconds,
  ROUND(u.avg_occupancy_pct, 1)  AS avg_occupancy_pct,
  COALESCE(u.total_incidents, 0) AS total_incidents,
  ROUND(u.avg_energy_kwh, 1)     AS avg_energy_kwh
FROM access a
LEFT JOIN mobility m ON m.bldg_uid = a.bldg_uid
LEFT JOIN udl      u ON u.bldg_uid = a.bldg_uid
""")

# COMMAND ----------

spark.sql(f"""
    SELECT * FROM {DA_GOLD}.building_activity
    ORDER BY access_events DESC
    LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Per-cell health — `gold.cell_health`
# MAGIC The transit/coverage counterpart: one row per cell fusing the **inventory**
# MAGIC (`wireless_cells`) with rolled-up **KPIs** (`wireless_node_health`). Same idea in PySpark
# MAGIC this time.

# COMMAND ----------

from pyspark.sql import functions as F

cells = spark.table(f"{DA_SILVER}.wireless_cells")
health = spark.table(f"{DA_SILVER}.wireless_node_health")

health_agg = (
    health.groupBy("cell_id")
    .agg(
        F.count("*").alias("health_records"),
        F.round(F.avg("prb_utilization_pct"), 1).alias("avg_prb_pct"),
        F.round(F.avg("active_users"), 0).alias("avg_active_users"),
        F.max("active_users").alias("peak_active_users"),
        F.round(F.avg("avg_latency_ms"), 1).alias("avg_latency_ms"),
        F.sum(F.when(F.col("alarm_code").isNotNull(), 1).otherwise(0)).alias("alarm_count"),
        F.round(100.0 * F.sum(F.when(F.col("status") == "online", 1).otherwise(0)) / F.count("*"), 1).alias("uptime_pct"),
    )
)

cell_health = (
    cells.select(
        "cell_id", "site_id", "site_name", "sector", "technology", "band",
        "latitude", "longitude", "vendor", "status",
    )
    .join(health_agg, "cell_id", "left")
)
cell_health.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{DA_GOLD}.cell_health"
)
display(cell_health.orderBy(F.desc("peak_active_users")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Verify
# MAGIC Confirm the Gold tables exist and are populated.

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {DA_GOLD}").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E. 🎯 Your turn
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC Build a third Gold table — <code>gold.building_connectivity</code> — with one row per building
# MAGIC summarizing Wi-Fi presence: average <code>connected_device_count</code>, total
# MAGIC <code>motion_events_1h</code>, and gateway count, from <code>connectivity_presence</code> joined to
# MAGIC <code>sc_gateway_network</code>. This becomes a "which buildings are busiest online" table for Genie.
# MAGIC </div>

# COMMAND ----------

# TODO: create {DA_GOLD}.building_connectivity

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Recap
# MAGIC You built **Gold** serving tables (`building_activity`, `cell_health`) that fuse multiple
# MAGIC datasets into one tidy row per entity. In **Lab 05** you'll point a **Genie space** at your
# MAGIC Silver + Gold tables and ask questions in plain English.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
