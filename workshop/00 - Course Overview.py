# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:20px 24px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:44px; height:44px; border-radius:10px; background:#FF3621; font-weight:800; font-size:20px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:44px; border-radius:10px; background:#0055B7; font-weight:800; font-size:16px;">UBC</span>
# MAGIC   <div>
# MAGIC     <div style="font-size:22px; font-weight:700;">Smart Community Testbed on Databricks</div>
# MAGIC     <div style="font-size:14px; opacity:0.85;">UBC Smart Community Workshop · Point Grey, Sept 2026</div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Smart Community Testbed on Databricks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This workshop takes you end-to-end on the Databricks Data Intelligence Platform using
# MAGIC **synthetic cellular-tower and smart-campus data** for the UBC Vancouver campus — the
# MAGIC same kind of data you'll work with in the UBC smart-community hackathon on **transit** and
# MAGIC **security** use cases.
# MAGIC
# MAGIC You will generate a realistic dataset, clean and conform it with the **medallion
# MAGIC architecture**, explore it with **AI/BI dashboards**, ask questions of it in plain
# MAGIC English with a **Genie space**, and finish by deploying a live **Databricks App** —
# MAGIC an interactive campus map over your own data — that you can extend for your hackathon
# MAGIC project.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC
# MAGIC By the end of this workshop, you will be able to:
# MAGIC
# MAGIC - Generate and land synthetic data as **Bronze** Delta tables in Unity Catalog
# MAGIC - Clean, type, and conform raw data into a trustworthy **Silver** layer with SQL and PySpark
# MAGIC - Build **Gold** serving tables and an **AI/BI dashboard** for transit & security signals
# MAGIC - Create and configure a **Genie space** for natural-language Q&A over your data
# MAGIC - Deploy a **Databricks App** (interactive campus map) backed by your Silver tables and Genie

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Workspace Setup Information
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size:1.05em;">Where to run this</strong>
# MAGIC <div style="color:#0d47a1;">
# MAGIC <ul style="margin:0; padding-left:20px;">
# MAGIC <li>Run every notebook on <strong>Serverless</strong> compute (see Requirements below).</li>
# MAGIC <li>Each lab's <strong>first cell</strong> runs a Classroom-Setup that creates your personal
# MAGIC     <code>labuser_&lt;username&gt;</code> catalog with <code>bronze</code> / <code>silver</code> /
# MAGIC     <code>gold</code> schemas. You only build in your own catalog — nothing is shared.</li>
# MAGIC <li>Run the labs <strong>in order</strong> (01 → 06). Each depends on the tables the previous one created.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size:1.05em;">Do not run in a production workspace</strong>
# MAGIC <div style="color:#333;">The setup scripts create catalogs, schemas, tables, a Genie space, and an App.
# MAGIC Use only the lab / sandbox workspace provided for this event.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Course Agenda
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <style>.agenda-table td, .agenda-table th { font-size: 13.5pt !important; }</style>
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 90px;">Type</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Module Name</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">What you build</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">01</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Explore &amp; Profile the Data</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">Query &amp; profile Bronze in UC</td></tr>
# MAGIC     <tr style="background: #fbfaf8;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">02</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Ingest Geodata &amp; Clean</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">dim tables from geodata + Silver</td></tr>
# MAGIC     <tr style="background: white;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">03</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Visualize with AI/BI</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">AI/BI dashboard</td></tr>
# MAGIC     <tr style="background: #fbfaf8;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">04</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Build Serving Tables</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">Gold aggregates for app &amp; Genie</td></tr>
# MAGIC     <tr style="background: white;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">05</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Create a Genie Space</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">Natural-language Q&amp;A space</td></tr>
# MAGIC     <tr style="background: #fbfaf8;"><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;font-weight:700;color:#1B5162;">06</td><td style="padding:8px 14px;border:1px solid #EEEDE9;text-align:center;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-weight:600;">Lab</span></td><td style="padding:8px 14px;border:1px solid #EEEDE9;">Deploy the App</td><td style="padding:8px 14px;border:1px solid #EEEDE9;color:#5B6B73;">Live single-tab campus-map app</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Requirements
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC - Compute: **`Serverless`** (attach via the **Connect** drop-down, top-right of each notebook).
# MAGIC - Run the labs **in order** — 01 creates the tables 02 cleans, and so on.
# MAGIC - Unity Catalog enabled, with permission to create a catalog (the setup handles this).
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
