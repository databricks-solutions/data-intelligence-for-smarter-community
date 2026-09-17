# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex; align-items:center; gap:16px; padding:18px 22px; background:linear-gradient(135deg,#1B3139 0%,#1B5162 100%); border-radius:12px; color:#fff; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; width:40px; height:40px; border-radius:10px; background:#FF3621; font-weight:800; font-size:18px;">R</span>
# MAGIC   <span style="display:inline-flex; align-items:center; justify-content:center; padding:0 12px; height:40px; border-radius:10px; background:#0055B7; font-weight:800; font-size:15px;">UBC</span>
# MAGIC   <div style="font-size:15px; font-weight:600; opacity:0.9;">Smart Community Testbed on Databricks · Lab 02</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 02 — Ingest the Geodata &amp; Clean to Silver
# MAGIC
# MAGIC Two jobs this lab. First, **ingest** the real UBC open geodata sitting in your Volume into
# MAGIC dimension tables (`dim_buildings`, `dim_zones`) — your first hands-on "load from a source."
# MAGIC Then promote all of Bronze (the telemetry from setup **plus** your new dimensions) into a
# MAGIC trustworthy **Silver** layer.
# MAGIC
# MAGIC ## Objectives
# MAGIC - **Ingest** the UBCGeodata GeoJSON from a Volume into `dim_buildings` + `dim_zones`
# MAGIC - Apply the core cleaning moves: **de-duplicate**, **validate**, **handle nulls**, **conform keys**
# MAGIC - Validate every building sits inside the **UBC bounding box**
# MAGIC - Write cleaned **Silver** tables that the app and Genie will read
# MAGIC
# MAGIC ## Agenda
# MAGIC **A.** Setup · **B.** Ingest the geodata · **C.** What "clean" means · **D.** Clean the dimensions · **E.** Clean an event stream · **F.** Conform the rest · **G.** 🎯 Your turn

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Setup
# MAGIC <div style="border-left:4px solid #FF3621; background:#FFF0EE; padding:12px 16px; border-radius:4px; margin:12px 0; color:#1B3139;">Attach <strong>Serverless</strong>, then run the setup cell. It provides the Bronze telemetry and the geodata Volume (<code>GEODATA_DIR</code>) — same as Lab 01.</div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Ingest the UBC open geodata
# MAGIC
# MAGIC The one piece of **real** data is the UBCGeodata in your Volume. We read the GeoJSON,
# MAGIC pull out the fields we want, compute a centroid for each polygon, and write two
# MAGIC **Bronze** dimension tables — `dim_buildings` and `dim_zones`. Crucially,
# MAGIC `ubcv_buildings.geojson` carries `BLDG_UID` (`VBL10xxx`) — the **same key** the telemetry
# MAGIC uses — so these dimensions join cleanly to everything else.

# COMMAND ----------

# DBTITLE 1,Helper — centroid of a GeoJSON Polygon / MultiPolygon
def centroid(geom):
    """Mean of all coordinate pairs (good enough to place a building on the map)."""
    t, c = geom["type"], geom["coordinates"]
    if t == "Polygon":
        rings = c
    elif t == "MultiPolygon":
        rings = [ring for poly in c for ring in poly]
    else:
        return (None, None)
    pts = [pt for ring in rings for pt in ring]
    lats = [p[1] for p in pts]
    lons = [p[0] for p in pts]
    return (sum(lats) / len(lats), sum(lons) / len(lons))

# BLDG_USAGE in the geodata -> the category vocabulary the telemetry uses.
CATEGORY_MAP = {
    "Housing": "Residential", "StudentHousing": "Residential", "Academic": "Academic",
    "Research": "Research", "Services": "Services", "Operations": "Operations",
    "Administrative": "Administrative", "Athletics": "Athletics", "Commons": "Commons",
    "Parking": "Parking", "Commercial": "Commercial",
}

# COMMAND ----------

# DBTITLE 1,Ingest ubcv_buildings.geojson -> dim_buildings
import json

with open(f"{GEODATA_DIR}/ubcv_buildings.geojson") as f:
    gj = json.load(f)

rows = []
for ft in gj["features"]:
    p = ft["properties"]
    if p.get("CONSTR_STATUS") != "Complete" or not p.get("BLDG_UID") or not p.get("NAME"):
        continue
    lat, lon = centroid(ft["geometry"])
    rows.append((
        p["BLDG_UID"], p["NAME"], p.get("BLDG_CODE"), p.get("PRIMARY_ADDRESS"),
        CATEGORY_MAP.get(p.get("BLDG_USAGE"), "Other"), p.get("NEIGHBOURHOOD"), lat, lon,
    ))

dim_buildings = spark.createDataFrame(
    rows,
    ["bldg_uid", "building_name", "building_code", "address", "category", "neighbourhood", "latitude", "longitude"],
)
dim_buildings.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DA_BRONZE}.dim_buildings")
print(f"dim_buildings: {dim_buildings.count()} buildings")
display(dim_buildings.limit(5))

# COMMAND ----------

# DBTITLE 1,Ingest ubcv_neighbourhoods.geojson -> dim_zones
with open(f"{GEODATA_DIR}/ubcv_neighbourhoods.geojson") as f:
    nj = json.load(f)

zrows = []
for i, ft in enumerate(nj["features"]):
    p = ft["properties"]
    lat, lon = centroid(ft["geometry"])
    zrows.append((str(p.get("ID") or i), p.get("NAME"), "neighbourhood", lat, lon))

dim_zones = spark.createDataFrame(zrows, ["zone_id", "zone_name", "zone_type", "latitude", "longitude"])
dim_zones.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DA_BRONZE}.dim_zones")
print(f"dim_zones: {dim_zones.count()} neighbourhoods")
display(dim_zones)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. What "clean" means here
# MAGIC
# MAGIC Silver isn't one magic step — it's a few disciplined checks applied consistently:
# MAGIC
# MAGIC | Move | Why it matters |
# MAGIC |---|---|
# MAGIC | **De-duplicate** on the natural key | Streams re-deliver events; counts must not double |
# MAGIC | **Validate coordinates** (UBC bbox) | A stray lat/lon puts a building in the ocean on the map |
# MAGIC | **Handle nulls** in join keys | A null `bldg_uid` can't join to a building |
# MAGIC | **Conform types & keys** | Every dataset must join on the same `bldg_uid` |
# MAGIC
# MAGIC UBC Vancouver bounding box: latitude **49.24–49.28 N**, longitude **−123.27 to −123.22 W**
# MAGIC (covers the full campus — the real building footprints extend east to ≈−123.228).

# COMMAND ----------

# DBTITLE 1,Reusable bounding-box check
from pyspark.sql import functions as F

UBC_LAT_MIN, UBC_LAT_MAX = 49.24, 49.28
UBC_LON_MIN, UBC_LON_MAX = -123.27, -123.22

def in_ubc_bbox(lat_col="latitude", lon_col="longitude"):
    return (
        F.col(lat_col).between(UBC_LAT_MIN, UBC_LAT_MAX)
        & F.col(lon_col).between(UBC_LON_MIN, UBC_LON_MAX)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Clean the dimensions — `dim_buildings`
# MAGIC The building dimension anchors the whole model, so we clean it first: keep one row per
# MAGIC `bldg_uid`, and only rows with valid UBC coordinates. (`dim_zones` is cleaned with the
# MAGIC rest in section F.)

# COMMAND ----------

bronze_bld = spark.table(f"{DA_BRONZE}.dim_buildings")
print(f"Bronze rows: {bronze_bld.count()}")

silver_bld = (
    bronze_bld
    .dropDuplicates(["bldg_uid"])          # de-duplicate on the natural key
    .filter(F.col("bldg_uid").isNotNull()) # key must exist
    .filter(in_ubc_bbox())                  # coordinates must be on campus
)
print(f"Silver rows after cleaning: {silver_bld.count()}")

silver_bld.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{DA_SILVER}.dim_buildings"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Clean an event stream — `sc_access_events`
# MAGIC Event feeds are the classic case for de-duplication (same `event_id` re-delivered) and
# MAGIC null-key handling (an event with no `bldg_uid` can't be attributed to a building).

# COMMAND ----------

# How many duplicate event_ids are in Bronze?
spark.sql(f"""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT event_id) AS distinct_events,
           COUNT(*) - COUNT(DISTINCT event_id) AS duplicates
    FROM {DA_BRONZE}.sc_access_events
""").display()

# COMMAND ----------

silver_access = (
    spark.table(f"{DA_BRONZE}.sc_access_events")
    .dropDuplicates(["event_id"])
    .filter(F.col("bldg_uid").isNotNull())
    .filter(F.col("event_ts").isNotNull())
)
silver_access.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{DA_SILVER}.sc_access_events"
)
print(f"Silver sc_access_events rows: {silver_access.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Conform the rest
# MAGIC The remaining tables get the same treatment: drop exact-duplicate rows and, where a table
# MAGIC has a `bldg_uid`, drop rows where it's null. We loop so the logic stays consistent — this
# MAGIC is how you'd promote many tables at once in a real pipeline.

# COMMAND ----------

# DBTITLE 1,Promote remaining Bronze tables to Silver
from pyspark.sql.utils import AnalysisException

REMAINING = [
    "dim_zones", "sc_device_health", "mobility_presence",
    "wireless_cells", "wireless_connectivity", "wireless_node_health",
    "connectivity_presence", "sc_gateway_network", "udl_campus_metrics",
]

results = []
for tbl in REMAINING:
    df = spark.table(f"{DA_BRONZE}.{tbl}").dropDuplicates()
    if "bldg_uid" in df.columns:
        df = df.filter(F.col("bldg_uid").isNotNull())
    # Cell towers are point locations — validate they sit on campus. (Zones are
    # neighbourhood areas whose centroid can sit near the bbox edge, so we don't
    # bbox-filter those.)
    if set(["latitude", "longitude"]).issubset(df.columns) and tbl == "wireless_cells":
        df = df.filter(in_ubc_bbox())
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DA_SILVER}.{tbl}")
    results.append((tbl, df.count()))

display(spark.createDataFrame(results, ["silver_table", "row_count"]).orderBy("silver_table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential-integrity check
# MAGIC A good Silver layer *joins*. Let's confirm every `bldg_uid` in the access stream matches a
# MAGIC building — this is the check that makes the campus map trustworthy.

# COMMAND ----------

spark.sql(f"""
    SELECT
      (SELECT COUNT(DISTINCT bldg_uid) FROM {DA_SILVER}.sc_access_events) AS access_buildings,
      (SELECT COUNT(*) FROM {DA_SILVER}.dim_buildings) AS dim_buildings,
      (SELECT COUNT(DISTINCT a.bldg_uid)
         FROM {DA_SILVER}.sc_access_events a
         LEFT ANTI JOIN {DA_SILVER}.dim_buildings b ON a.bldg_uid = b.bldg_uid) AS unmatched_buildings
""").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G. 🎯 Your turn
# MAGIC <div style="border-left:4px solid #00A972; background:#E8F5E9; padding:14px 18px; border-radius:4px; margin:12px 0; color:#1B3139;">
# MAGIC The <code>udl_campus_metrics</code> table is a <strong>daily</strong> feed (one row per building per day).
# MAGIC Write a check that finds any <code>occupancy_pct</code> values outside the valid <strong>0–100</strong>
# MAGIC range in your Silver table. If you find bad values, how would you clean them — clamp, or drop?
# MAGIC </div>

# COMMAND ----------

# TODO: find occupancy_pct values outside 0–100 in {DA_SILVER}.udl_campus_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Recap
# MAGIC You promoted 11 Bronze tables to a cleaned, conformed **Silver** layer: de-duplicated,
# MAGIC coordinate-validated, null-key-filtered, and referentially sound. **Lab 03** visualizes
# MAGIC this Silver data in an **AI/BI dashboard**.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
