# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 03</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 03 — Visualize with AI/BI
# MAGIC
# MAGIC Your Silver data is clean and joinable. Now let's *see* it. **AI/BI Dashboards** let you
# MAGIC build interactive charts on Unity Catalog data — and the AI **assistant** can draft
# MAGIC visualizations for you from a plain-language ask.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Create an **AI/BI dashboard** and connect it to your Silver tables
# MAGIC - Build charts for the two hackathon themes: a **transit** view and a **security** view
# MAGIC - Use the dashboard **assistant** to generate a visualization from a prompt
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** Preview the queries · **C.** Create the dashboard (UI) · **D.** Add charts · **E.** Ask the assistant · **F.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border-left:4px solid #FF3621; background:#FFF0EE; padding:12px 16px; border-radius:4px; margin:12px 0; color:#1B3139;">Attach <strong>Serverless</strong> and run setup. This lab reads your <strong>Silver</strong> tables from Lab 02.</div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Preview the queries
# MAGIC
# MAGIC A dashboard is just saved SQL + charts. Let's validate the two queries here first, then
# MAGIC paste them into the dashboard. Note your fully-qualified schema — you'll need it in the UI:

# COMMAND ----------

print("Your Silver schema (use this in the dashboard):", DA_SILVER)

# COMMAND ----------

# DBTITLE 1,Transit signal — hourly mobility across campus
# Hourly unique devices (a transit / footfall signal). Copy this SQL (with your schema
# from the print above) into the dashboard's "Create from SQL" in section C.
spark.sql(f"""
    SELECT date_trunc('hour', ts) AS hour,
           COUNT(DISTINCT anon_device_id) AS unique_devices,
           AVG(dwell_seconds) AS avg_dwell_seconds
    FROM {DA_SILVER}.mobility_presence
    GROUP BY 1
    ORDER BY 1
""").display()

# COMMAND ----------

# DBTITLE 1,Security signal — forced-door and tailgate events by building
# Security anomalies by building.
spark.sql(f"""
    SELECT building_name,
           SUM(CASE WHEN event_type = 'door_forced'       THEN 1 ELSE 0 END) AS forced_doors,
           SUM(CASE WHEN event_type = 'tailgate_detected' THEN 1 ELSE 0 END) AS tailgates,
           COUNT(*) AS total_events
    FROM {DA_SILVER}.sc_access_events
    GROUP BY building_name
    HAVING forced_doors > 0 OR tailgates > 0
    ORDER BY forced_doors DESC, tailgates DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Create the dashboard (in the UI)
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2; background:#e3f2fd; padding:14px 18px; border-radius:4px; margin:12px 0; color:#0d47a1;">
# MAGIC <ol style="margin:0; padding-left:20px; line-height:1.8;">
# MAGIC   <li>In the left sidebar, click <strong>Dashboards</strong> → <strong>Create dashboard</strong>.</li>
# MAGIC   <li>Name it <strong>Smart Campus — &lt;your name&gt;</strong>.</li>
# MAGIC   <li>Open the <strong>Data</strong> tab → <strong>Create from SQL</strong>. Paste the
# MAGIC       <em>Transit signal</em> query from section B (replace <code>${DA_SILVER}</code> with the
# MAGIC       schema printed above). Name the dataset <code>mobility_hourly</code>. Click <strong>Run</strong>.</li>
# MAGIC   <li>Add a second dataset the same way for the <em>Security signal</em> query; name it
# MAGIC       <code>security_by_building</code>.</li>
# MAGIC </ol>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Add charts
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2; background:#e3f2fd; padding:14px 18px; border-radius:4px; margin:12px 0; color:#0d47a1;">
# MAGIC On the <strong>Canvas</strong> tab, click <strong>Add a visualization</strong> and drag out a widget:
# MAGIC <ul style="margin:8px 0 0 0; padding-left:20px; line-height:1.8;">
# MAGIC   <li><strong>Line chart</strong> on <code>mobility_hourly</code>: X = <code>hour</code>, Y = <code>unique_devices</code>. This is your campus footfall over time — the transit pulse.</li>
# MAGIC   <li><strong>Bar chart</strong> on <code>security_by_building</code>: X = <code>building_name</code>, Y = <code>forced_doors</code>. The buildings with the most forced-door events.</li>
# MAGIC   <li><strong>Counter</strong> on <code>security_by_building</code>: sum of <code>total_events</code>.</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Ask the assistant
# MAGIC
# MAGIC <div style="border-left:4px solid #8E7CC3; background:#F3EEFB; padding:14px 18px; border-radius:4px; margin:12px 0; color:#4A2C7A;">
# MAGIC Add a visualization, then click the <strong>✨ assistant</strong> and type a plain-language ask, e.g.:
# MAGIC <br/><br/>
# MAGIC <em>"Show average dwell time by hour of day"</em> &nbsp;·&nbsp; <em>"Top 10 buildings by total access events"</em>
# MAGIC <br/><br/>
# MAGIC The assistant proposes a chart from your dataset — accept it or refine the prompt. This is the
# MAGIC same idea you'll use in Lab 05 with Genie, but scoped to a single dashboard widget.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F. 🎯 Your turn
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC Add a third dataset + chart of your own from any Silver table. Ideas:
# MAGIC <code>udl_campus_metrics</code> daily occupancy trend, <code>wireless_node_health</code> cells by status,
# MAGIC or <code>connectivity_presence</code> Wi-Fi devices by building. Give the dashboard a filter on
# MAGIC <code>category</code> so viewers can slice by building type.
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Optional — a query to seed your third chart
spark.sql(f"""
    SELECT date, AVG(occupancy_pct) AS avg_occupancy, SUM(safety_incidents_count) AS incidents
    FROM {DA_SILVER}.udl_campus_metrics
    GROUP BY date
    ORDER BY date
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Recap
# MAGIC You built an interactive **AI/BI dashboard** on your Silver data with transit and security
# MAGIC views, and used the assistant to draft a chart from a prompt. **Lab 04** builds **Gold**
# MAGIC serving tables — tidy per-building aggregates that make Genie (Lab 05) and your app precise.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
