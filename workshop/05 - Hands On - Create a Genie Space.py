# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 05</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 05 — Create a Genie Space
# MAGIC
# MAGIC **Genie** lets anyone ask questions of governed data in plain English and get back SQL,
# MAGIC tables, and charts. You'll point a Genie space at your Silver + Gold tables, teach it a
# MAGIC little context, and ask transit & security questions — then capture its **Space ID** so
# MAGIC your app (Lab 06) can use it.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Create a **Genie space** scoped to your workshop tables
# MAGIC - Improve accuracy with **instructions** and **sample questions**
# MAGIC - Ask natural-language questions and read the generated SQL
# MAGIC - Capture the **Space ID** for Lab 06
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** Create the space (UI) · **C.** Add context · **D.** Ask questions · **E.** Capture the Space ID · **F.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border-left:4px solid #FF3621; background:#FFF0EE; padding:12px 16px; border-radius:4px; margin:12px 0; color:#1B3139;">Attach <strong>Serverless</strong> and run setup. Needs your <strong>Silver</strong> (Lab 02) and <strong>Gold</strong> (Lab 04) tables.</div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05

# COMMAND ----------

print("Point your Genie space at these schemas:")
print("  Silver:", DA_SILVER)
print("  Gold:  ", DA_GOLD)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Create the space (in the UI)
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2; background:#e3f2fd; padding:14px 18px; border-radius:4px; margin:12px 0; color:#0d47a1;">
# MAGIC <ol style="margin:0; padding-left:20px; line-height:1.8;">
# MAGIC   <li>Left sidebar → <strong>Genie</strong> → <strong>New</strong>.</li>
# MAGIC   <li>Name it <strong>Smart Campus — &lt;your name&gt;</strong>.</li>
# MAGIC   <li>Choose your <strong>SQL warehouse</strong> (Serverless).</li>
# MAGIC   <li>Add tables from your <strong>Gold</strong> schema first — <code>building_activity</code> and
# MAGIC       <code>cell_health</code> — then add key <strong>Silver</strong> tables:
# MAGIC       <code>sc_access_events</code>, <code>mobility_presence</code>, <code>udl_campus_metrics</code>,
# MAGIC       <code>dim_buildings</code>.</li>
# MAGIC </ol>
# MAGIC Genie is best with a <strong>focused</strong> set of well-named tables — the Gold tables you built
# MAGIC in Lab 04 are ideal because their columns already read like plain English.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Add context (instructions + sample questions)
# MAGIC
# MAGIC In the space's **Instructions** panel, paste something like this so Genie understands the domain:
# MAGIC
# MAGIC <div style="border:1px solid #DCE0E2; border-radius:8px; background:#F9F7F4; padding:14px 18px; margin:12px 0; color:#1B3139; font-size:0.92rem;">
# MAGIC This data is a synthetic smart-community testbed for the UBC Vancouver campus.
# MAGIC Buildings are keyed by <code>bldg_uid</code> (equals <code>dim_buildings.building_id</code>).
# MAGIC "Security" questions use <code>sc_access_events</code> (forced_doors = event_type 'door_forced';
# MAGIC tailgates = 'tailgate_detected'). "Transit / footfall" questions use <code>mobility_presence</code>
# MAGIC (unique devices = distinct anon_device_id) and <code>cell_health</code>. Occupancy, energy, and
# MAGIC safety incidents come from <code>udl_campus_metrics</code>. Prefer the Gold tables
# MAGIC <code>building_activity</code> and <code>cell_health</code> for per-building / per-cell summaries.
# MAGIC </div>
# MAGIC
# MAGIC Then add these as **Sample Questions**:
# MAGIC
# MAGIC - *Which 5 buildings have the most forced-door events?*
# MAGIC - *What is the busiest hour for unique devices on campus?*
# MAGIC - *Show average occupancy by building category.*
# MAGIC - *Which cell sites have the highest peak active users?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Ask questions
# MAGIC Open the space and try the sample questions plus your own. For each answer, expand the
# MAGIC **generated SQL** — comparing Genie's SQL to what you'd write is the fastest way to trust it.
# MAGIC Try a follow-up like *"...only for residential buildings"* to see it keep context.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Capture the Space ID
# MAGIC
# MAGIC <div style="border-left:4px solid #FFAB00; background:#FFF8E1; padding:14px 18px; border-radius:4px; margin:12px 0; color:#5D4037;">
# MAGIC Your app in <strong>Lab 06</strong> needs this space's ID. Look at the URL of your open space:
# MAGIC <br/><code>https://&lt;workspace&gt;/genie/rooms/<strong>01efab...</strong></code> — the part after
# MAGIC <code>/rooms/</code> is the Space ID. Copy it and paste it below, then run the cell to save it.
# MAGIC </div>

# COMMAND ----------

dbutils.widgets.text("genie_space_id", "", "Your Genie Space ID (from the space URL)")
genie_space_id = dbutils.widgets.get("genie_space_id").strip()

if genie_space_id:
    # Persist it so Lab 06 can read it back without re-typing.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA_GOLD.rsplit('.',1)[0]}._workshop_config")
    spark.createDataFrame([(genie_space_id,)], "genie_space_id string").write.mode("overwrite").saveAsTable(
        f"{DA_CATALOG}._workshop_config.genie"
    )
    print(f"✅ Saved Genie Space ID: {genie_space_id}")
    print("   Lab 06 will read it from {catalog}._workshop_config.genie")
else:
    print("ℹ️ Paste your Space ID into the widget above and re-run this cell.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F. 🎯 Your turn
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC Ask Genie a question it gets <em>wrong</em> or vague, then improve it: add a clarifying
# MAGIC <strong>instruction</strong> or a <strong>SQL example</strong> to the space and re-ask. Getting Genie
# MAGIC from "almost" to "correct" by adding context is the core skill for your hackathon demo.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Recap
# MAGIC You created and tuned a **Genie space** over your governed tables and captured its Space ID.
# MAGIC In **Lab 06** you deploy the campus-map **app** — wired to your Silver tables and this Genie
# MAGIC space, so users can explore the map *and* ask it questions.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
