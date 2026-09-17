"""
Smart Community Testbed - Consolidated Data Generator
===================================================================

Generates all 11 tables for the workshop in a single reusable module.
Engine: pandas + NumPy -> spark.createDataFrame with EXPLICIT StructType.

Entry point:
    generate_all(spark, catalog, schema, scale=0.15, seed=42, geojson_dir=None)
        -> returns dict {table_name: row_count}

Tables produced:
  1. dim_buildings (38 rows, fixed)
  2. dim_zones (16 rows, fixed)
  3. sc_access_events (~90K at scale=0.15, ~600K at full scale)
  4. sc_device_health (~37.5K at scale=0.15, ~250K at full scale)
  5. mobility_presence (~75K at scale=0.15, ~500K at full scale)
  6. wireless_cells (~250 rows, fixed)
  7. wireless_connectivity (~30K at scale=0.15, ~200K at full scale)
  8. wireless_node_health (~100K+ rows per scale, fixed cell count)
  9. connectivity_presence (~60K at scale=0.15, ~400K at full scale)
 10. sc_gateway_network (~40.5K at scale=0.15, ~270K at full scale)
 11. udl_campus_metrics (~1.95K at scale=0.15, ~13K at full scale)

Seed: deterministic per scale, matching the reference build.
Time window: 30 days from 2026-04-16.
Geospatial: UBC campus bbox 49.24-49.28N / -123.27 to -123.24W.
"""

import hashlib
import json
import math
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType,
    BooleanType, TimestampType, DateType,
)

# ============================================================================
# Configuration + Constants
# ============================================================================
SEED = 42
NOW = np.datetime64("2026-04-16T00:00:00")
WINDOW_START = NOW - np.timedelta64(30, "D")
WINDOW_SECONDS = int((NOW - WINDOW_START).astype("timedelta64[s]").astype(int))

STUDY_CRUNCH_START = np.datetime64("2026-04-05T00:00:00")
STUDY_CRUNCH_END = np.datetime64("2026-04-12T23:59:59")

# UBC campus bounding box
UBC_LAT_MIN, UBC_LAT_MAX = 49.2400, 49.2800
UBC_LON_MIN, UBC_LON_MAX = -123.2700, -123.2380

# Full-scale row counts (before scale factor applied)
FULL_SCALE_ACCESS_EVENTS = 600_000
FULL_SCALE_DEVICE_HEALTH = 250_000
FULL_SCALE_MOBILITY = 500_000
FULL_SCALE_WIRELESS_CONN = 200_000
FULL_SCALE_CONNECTIVITY = 400_000
FULL_SCALE_GATEWAY = 270_000
FULL_SCALE_UDL = 13_000

# Zone definitions (16 fixed zones on campus)
UBC_ZONES = [
    ("ZONE_01", "AMS Student Nest",             "transit",     49.2665, -123.2500),
    ("ZONE_02", "Irving K. Barber Learning Centre","academic", 49.2670, -123.2535),
    ("ZONE_03", "Koerner Library",              "academic",    49.2660, -123.2560),
    ("ZONE_04", "UBC Aquatic Centre",           "recreational",49.2688, -123.2486),
    ("ZONE_05", "UBC Bookstore",                "academic",    49.2668, -123.2510),
    ("ZONE_06", "Thunderbird Arena",            "recreational",49.2570, -123.2445),
    ("ZONE_07", "War Memorial Gym",             "recreational",49.2650, -123.2476),
    ("ZONE_08", "UBC Bus Loop",                 "transit",     49.2684, -123.2483),
    ("ZONE_09", "Wesbrook Village",             "residential", 49.2520, -123.2380),
    ("ZONE_10", "Life Sciences Institute",      "academic",    49.2635, -123.2447),
    ("ZONE_11", "Main Mall Central",            "outdoor",     49.2640, -123.2548),
    ("ZONE_12", "Rose Garden",                  "outdoor",     49.2695, -123.2565),
    ("ZONE_13", "MacInnes Field",               "outdoor",     49.2640, -123.2485),
    ("ZONE_14", "Nitobe Memorial Garden",       "outdoor",     49.2630, -123.2580),
    ("ZONE_15", "UBC Farm",                     "outdoor",     49.2500, -123.2380),
    ("ZONE_16", "Student Union Blvd Transit",   "transit",     49.2695, -123.2515),
]

# Cell sites (10 synthetic locations)
CELL_SITES = [
    {"site_id": "SITE_01", "name": "UBC Bus Loop",              "latitude": 49.2695, "longitude": -123.2498},
    {"site_id": "SITE_02", "name": "Main Mall / Buchanan",      "latitude": 49.2640, "longitude": -123.2548},
    {"site_id": "SITE_03", "name": "Wesbrook & University",     "latitude": 49.2626, "longitude": -123.2383},
    {"site_id": "SITE_04", "name": "Thunderbird Stadium",       "latitude": 49.2577, "longitude": -123.2445},
    {"site_id": "SITE_05", "name": "Acadia Park",               "latitude": 49.2550, "longitude": -123.2475},
    {"site_id": "SITE_06", "name": "UBC Hospital",              "latitude": 49.2648, "longitude": -123.2422},
    {"site_id": "SITE_07", "name": "University Village",        "latitude": 49.2657, "longitude": -123.2350},
    {"site_id": "SITE_08", "name": "Rose Garden / Cecil Green", "latitude": 49.2700, "longitude": -123.2578},
    {"site_id": "SITE_09", "name": "Marine Drive Residence",    "latitude": 49.2627, "longitude": -123.2590},
    {"site_id": "SITE_10", "name": "Totem Park",                "latitude": 49.2565, "longitude": -123.2537},
]

USAGE_TO_CATEGORY: Dict[str, str] = {
    "Housing": "Residential", "StudentHousing": "Residential",
    "Academic": "Academic", "Services": "Services", "Research": "Research",
    "Operations": "Operations", "Administrative": "Administrative",
    "Athletics": "Athletics", "Commons": "Commons", "Parking": "Parking",
    "Commercial": "Commercial", "Other": "Other",
}

CATEGORY_EVENT_WEIGHT: Dict[str, float] = {
    "Residential": 1.0, "Academic": 1.2, "Commons": 1.5, "Athletics": 0.6,
    "Services": 0.7, "Research": 0.4, "Administrative": 0.3, "Operations": 0.3,
    "Parking": 0.4, "Commercial": 0.5, "Other": 0.2,
}

TECH_BANDS: Dict[str, List[Tuple[str, float, float]]] = {
    "2G_GSM": [("B5_850", 869.0, 5.0), ("B2_1900", 1960.0, 5.0)],
    "3G_UMTS": [("B5_850", 881.5, 5.0), ("B2_1900", 1962.5, 5.0), ("B4_AWS", 2132.5, 5.0)],
    "4G_LTE": [("B12_700", 731.0, 10.0), ("B5_850", 869.0, 10.0), ("B4_AWS", 2132.5, 15.0),
               ("B7_2600", 2655.0, 20.0), ("B66_AWS_ext", 2170.0, 15.0)],
    "5G_NR": [("n71_600", 623.0, 10.0), ("n78_3500", 3550.0, 100.0)],
}

TECH_SITE_PROB = {
    "2G_GSM": 0.50, "3G_UMTS": 0.70, "4G_LTE": 1.00, "5G_NR": 0.70,
}

SECTORS = [("α", 0), ("β", 120), ("γ", 240)]
VENDORS = ["Ericsson", "Nokia", "Samsung"]

# ============================================================================
# Helper Functions
# ============================================================================

def _categorize(usage: Optional[str]) -> str:
    if usage is None:
        return "Other"
    s = str(usage).strip() or "Other"
    return USAGE_TO_CATEGORY.get(s, "Other")


def _polygon_centroid(geom: Dict) -> Tuple[float, float]:
    """Compute centroid lat/lon from GeoJSON Polygon or MultiPolygon."""
    coords: List[List[float]] = []
    gtype = geom.get("type")
    raw = geom.get("coordinates") or []
    if gtype == "Polygon" and raw:
        coords = raw[0]
    elif gtype == "MultiPolygon" and raw and raw[0]:
        coords = raw[0][0]
    if not coords:
        return 49.2606, -123.2460
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return float(sum(lats) / len(lats)), float(sum(lons) / len(lons))


def _jitter(lat: np.ndarray, lon: np.ndarray, rng: np.random.Generator,
           scale_m: float = 40.0) -> Tuple[np.ndarray, np.ndarray]:
    """Add spatial jitter to coordinates."""
    dlat = rng.normal(0, scale_m / 111_000.0, size=len(lat))
    dlon = rng.normal(0, scale_m / 73_000.0, size=len(lon))
    out_lat = np.clip(lat + dlat, UBC_LAT_MIN, UBC_LAT_MAX)
    out_lon = np.clip(lon + dlon, UBC_LON_MIN, UBC_LON_MAX)
    return out_lat.round(6), out_lon.round(6)


def _distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in meters between two lat/lon points."""
    m_per_lat = 111_132.954
    m_per_lon = 73_000.0  # approx at UBC latitude
    dy = (lat1 - lat2) * m_per_lat
    dx = (lon1 - lon2) * m_per_lon
    return math.hypot(dx, dy)


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compass bearing (0=N) from point1 to point2, degrees."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


# ============================================================================
# GeoJSON Loading
# ============================================================================

def _load_buildings(geojson_path: Path) -> List[Dict]:
    """Load all complete buildings from GeoJSON."""
    with geojson_path.open("r", encoding="utf-8") as f:
        gj = json.load(f)
    rows: List[Dict] = []
    for feat in gj.get("features", []):
        p = feat.get("properties") or {}
        uid = p.get("BLDG_UID")
        name = p.get("NAME")
        status = p.get("CONSTR_STATUS")
        if not uid or not name or status != "Complete":
            continue
        geom = feat.get("geometry")
        if not geom:
            continue
        lat, lon = _polygon_centroid(geom)
        try:
            max_floors = int(p.get("MAX_FLOORS") or 1)
        except (TypeError, ValueError):
            max_floors = 1
        max_floors = max(1, max_floors)
        rows.append({
            "bldg_uid": uid,
            "building_name": name,
            "category": _categorize(p.get("BLDG_USAGE")),
            "max_floors": max_floors,
            "latitude": lat,
            "longitude": lon,
        })
    return rows


def _load_residences(geojson_path: Path) -> List[Dict]:
    """Load 38 residence complexes from ubcv_complexes.geojson."""
    complexes_path = geojson_path.parent / "ubcv_complexes.geojson"
    if not complexes_path.exists():
        return []
    with complexes_path.open("r", encoding="utf-8") as f:
        gj = json.load(f)
    rows: List[Dict] = []
    for feat in gj.get("features", []):
        p = feat.get("properties") or {}
        uid = p.get("BCOM_UID")
        name = p.get("NAME")
        if not uid or not name:
            continue
        geom = feat.get("geometry")
        if not geom:
            continue
        lat, lon = _polygon_centroid(geom)
        try:
            gba = int(p.get("GBA") or 0)
            units = int(p.get("UNITS") or 0)
        except (TypeError, ValueError):
            gba, units = 0, 0
        usage = p.get("USAGE_TYPE") or "StudentHousing"
        rows.append({
            "bcom_uid": uid,
            "name": name,
            "usage": usage,
            "gba": gba,
            "latitude": lat,
            "longitude": lon,
            "estimated_units": units,
        })
    return rows


# ============================================================================
# Main Generate Function
# ============================================================================

def generate_telemetry(spark, catalog: str, schema: str, scale: float = 0.15,
                       seed: int = 42, geojson_dir: str = None) -> Dict[str, int]:
    """
    Generate the 9 synthetic *telemetry* Bronze tables for the smart-community testbed.

    The two dimension tables (dim_buildings, dim_zones) are intentionally NOT
    produced here — students build those from the real UBC open geodata in Lab 02.
    Building references in the telemetry (bldg_uid = VBL10xxx, name, category,
    coordinates) are drawn from the same ubcv_buildings.geojson, so they join
    cleanly to the dim_buildings the students ingest.

    Args:
        spark: SparkSession (ambient, not created here)
        catalog: catalog name (e.g., "labuser_<username>")
        schema: schema name (e.g., "bronze")
        scale: row count multiplier (0.15 = ~15% of full)
        seed: random seed
        geojson_dir: path to geojson dir (relative to module if None)

    Returns:
        dict of {table_name: row_count}
    """
    print(f"\n{'=' * 72}")
    print("Smart Community Testbed - Workshop Generator")
    print(f"{'=' * 72}")
    print(f"Catalog: {catalog}.{schema}")
    print(f"Scale: {scale} (full = 1.0)")
    print(f"Seed: {seed}")
    print(f"Window: {WINDOW_START} to {NOW}")
    print()

    # Resolve GeoJSON directory
    if geojson_dir is None:
        module_dir = Path(__file__).parent
        geojson_dir = module_dir / "geojson"
    else:
        geojson_dir = Path(geojson_dir)

    buildings_geojson = geojson_dir / "ubcv_buildings.geojson"
    if not buildings_geojson.exists():
        raise FileNotFoundError(f"GeoJSON not found: {buildings_geojson}")

    rng = np.random.default_rng(seed)

    # Load reference data
    print(f"Loading GeoJSON from {geojson_dir}...")
    all_buildings = _load_buildings(buildings_geojson)
    residences = _load_residences(buildings_geojson)
    n_buildings = len(all_buildings)
    print(f"  Loaded {n_buildings} buildings")
    print(f"  Loaded {len(residences)} residence complexes")

    # Create schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Generate event tables (scaled). Dimensions come from the geodata in Lab 02.
    print("\n[1/9] Generating sc_access_events...")
    sc_access_df = _gen_sc_access_events(all_buildings, rng, scale)

    print("\n[2/9] Generating sc_device_health...")
    sc_device_df = _gen_sc_device_health(all_buildings, rng, scale)

    print("\n[3/9] Generating mobility_presence...")
    mobility_df = _gen_mobility_presence(all_buildings, rng, scale)

    print("\n[4/9] Generating wireless_cells...")
    wireless_cells_df = _gen_wireless_cells(rng)

    print("\n[5/9] Generating wireless_connectivity...")
    wireless_conn_df = _gen_wireless_connectivity(all_buildings, wireless_cells_df, rng, scale)

    print("\n[6/9] Generating wireless_node_health...")
    wireless_health_df = _gen_wireless_node_health(wireless_cells_df, rng)

    print("\n[7/9] Generating connectivity_presence...")
    connectivity_df = _gen_connectivity_presence(all_buildings, rng, scale)

    print("\n[8/9] Generating sc_gateway_network...")
    gateway_df = _gen_sc_gateway_network(all_buildings, rng, scale)

    print("\n[9/9] Generating udl_campus_metrics...")
    udl_df = _gen_udl_campus_metrics(all_buildings, rng, scale)

    # Write all to UC
    print(f"\n{'=' * 72}")
    print("Writing to Unity Catalog...")
    print(f"{'=' * 72}\n")

    tables = {
        "sc_access_events": (sc_access_df, _schema_sc_access_events()),
        "sc_device_health": (sc_device_df, _schema_sc_device_health()),
        "mobility_presence": (mobility_df, _schema_mobility_presence()),
        "wireless_cells": (wireless_cells_df, _schema_wireless_cells()),
        "wireless_connectivity": (wireless_conn_df, _schema_wireless_connectivity()),
        "wireless_node_health": (wireless_health_df, _schema_wireless_node_health()),
        "connectivity_presence": (connectivity_df, _schema_connectivity_presence()),
        "sc_gateway_network": (gateway_df, _schema_sc_gateway_network()),
        "udl_campus_metrics": (udl_df, _schema_udl_campus_metrics()),
    }

    counts = {}
    for table_name, (df, schema_obj) in tables.items():
        fqn = f"{catalog}.{schema}.{table_name}"
        n_rows = len(df)
        print(f"  Writing {fqn} ({n_rows:,} rows)...")
        df = _coerce_int_columns(df, schema_obj)  # NaN -> None for nullable INT/LONG cols
        sdf = spark.createDataFrame(df, schema=schema_obj)
        sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
        counts[table_name] = n_rows
        print(f"     OK")

    print(f"\n{'=' * 72}")
    print("DONE")
    print(f"{'=' * 72}\n")
    for tbl, cnt in counts.items():
        print(f"  {tbl:<30} = {cnt:>10,} rows")
    print()

    return counts


def _coerce_int_columns(df: pd.DataFrame, schema_obj) -> pd.DataFrame:
    """spark.createDataFrame with an explicit IntegerType/LongType rejects NaN.
    Several columns are legitimately nullable (floor for non-residential buildings;
    lac/enodeb_id/gnb_id only apply to certain cell technologies). Convert those
    columns to Python int / None so Spark maps missing values to SQL NULL."""
    df = df.copy()
    for field in schema_obj.fields:
        if isinstance(field.dataType, (IntegerType, LongType)) and field.name in df.columns:
            # Force object dtype: assigning a plain list of [None, 5, None] would make
            # pandas re-infer float64 (turning None back into NaN). A dtype=object Series
            # keeps None as None and ints as ints, which Spark maps to NULL / integer.
            df[field.name] = pd.Series(
                [None if pd.isna(v) else int(v) for v in df[field.name]],
                dtype=object,
                index=df.index,
            )
    return df


# ============================================================================
# Table Generators (stub implementations + schemas)
# ============================================================================

def _gen_dim_buildings(residences: List[Dict], rng: np.random.Generator) -> pd.DataFrame:
    """Generate 38 residence complexes (fixed)."""
    if not residences:
        residences = _default_residences()
    rows = []
    for i, r in enumerate(residences[:38], start=1):
        rows.append({
            "building_id": f"BLDG_{i:03d}",
            "bcom_uid": r.get("bcom_uid", ""),
            "building_name": r.get("name", ""),
            "address": None,
            "latitude": float(r.get("latitude", 49.2606)),
            "longitude": float(r.get("longitude", -123.2460)),
            "n_units": int(r.get("estimated_units", 100)),
            "complex_type": "residential",
            "year_built": 2000 + rng.integers(0, 20),
        })
    return pd.DataFrame(rows)


def _gen_dim_zones() -> pd.DataFrame:
    """Generate 16 campus zones (fixed)."""
    rows = [
        {"zone_id": z[0], "zone_name": z[1], "zone_type": z[2],
         "latitude": z[3], "longitude": z[4]}
        for z in UBC_ZONES
    ]
    return pd.DataFrame(rows)


def _gen_sc_access_events(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate smart-community access events."""
    n = int(FULL_SCALE_ACCESS_EVENTS * scale)
    rows = []
    # Security anomalies (door_forced, tailgate_detected) are rare but present so the
    # "security" theme in Labs 03-05 and the app has real signal to surface.
    event_types = ["door_open", "door_close", "credential_fail", "door_forced", "tailgate_detected"]
    event_probs = [0.40, 0.39, 0.15, 0.03, 0.03]
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        event_type = str(rng.choice(event_types, p=event_probs))
        # Keep door_state / success coherent with the event so the story reads correctly.
        if event_type == "door_forced":
            door_state, success = "open", False
        elif event_type == "tailgate_detected":
            door_state, success = "open", True   # tailgater slips in behind a valid entry
        elif event_type == "credential_fail":
            door_state, success = rng.choice(["closed", "locked"]), False
        else:  # door_open / door_close
            door_state = "open" if event_type == "door_open" else "closed"
            success = rng.random() < 0.98
        rows.append({
            "event_id": f"EVT_{uuid.uuid4().hex[:16]}",
            "event_ts": pd.Timestamp(ts),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "unit_id": f"UNIT_{b['bldg_uid']}_001",
            "device_id": f"DEV_{i:06d}",
            "occupant_id": f"RES_{i:06d}" if rng.random() < 0.9 else None,
            "event_type": event_type,
            "door_state": door_state,
            "credential_type": rng.choice(["fob", "app", "pin"]),
            "success": bool(success),
            "retries": int(rng.poisson(0.3)),
            "address": None,
            "latitude": b["latitude"],
            "longitude": b["longitude"],
            "floor": None,
        })
    return pd.DataFrame(rows)


def _gen_sc_device_health(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate smart-community device health snapshots."""
    n = int(FULL_SCALE_DEVICE_HEALTH * scale)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        rows.append({
            "device_id": f"DEV_{i:06d}",
            "device_type": rng.choice(["camera", "intercom", "access_panel", "door_sensor"]),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "floor": None,
            "ts": pd.Timestamp(ts),
            "status": rng.choice(["online", "degraded", "offline"], p=[0.95, 0.04, 0.01]),
            "firmware_version": f"v{rng.integers(1, 4)}.{rng.integers(0, 10)}.{rng.integers(0, 10)}",
            "uptime_seconds": int(rng.integers(60, 30*86400)),
            "fault_code": f"FC-E{rng.integers(1, 100):03d}" if rng.random() < 0.03 else None,
            "last_reboot_ts": pd.Timestamp(ts - rng.integers(1, 30).astype("timedelta64[D]")),
            "packet_loss_pct": float(np.clip(rng.beta(1.5, 20) * 100, 0, 15)),
            "temperature_c": float(np.clip(25.0 + rng.normal(0, 2.5), 10.0, 65.0)),
        })
    return pd.DataFrame(rows)


def _gen_mobility_presence(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate mobility_presence records distributed across all campus buildings.

    Uses the same building set (and `bldg_uid` namespace) as the other telemetry
    tables so it joins cleanly to sc_access_events for coordinates in the app's
    mobility heatmap and to the Gold building_activity table.
    """
    n = int(FULL_SCALE_MOBILITY * scale)
    # A pool of anonymized devices smaller than the record count, so
    # COUNT(DISTINCT anon_device_id) per building is meaningful (repeat visits).
    device_pool = max(1, n // 3)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        rows.append({
            "record_id": f"MOB_{i:09d}",
            "ts": pd.Timestamp(ts),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "anon_device_id": hashlib.sha256(f"dev_{i % device_pool}".encode()).hexdigest()[:12],
            "source_cell_id": "CELL_01_α_4G_LTE_B12_700",
            "dwell_seconds": int(rng.integers(300, 7200)),
            "device_count_in_building": int(rng.integers(1, 50)),
            "signal_strength_dbm": float(np.clip(rng.normal(-70, 10), -120, -30)),
            "arrival_flag": rng.random() < 0.25,
            "departure_flag": rng.random() < 0.25,
            "movement_type": rng.choice(["stationary", "walking", "vehicle"]),
        })
    return pd.DataFrame(rows)


def _gen_wireless_cells(rng: np.random.Generator) -> pd.DataFrame:
    """Generate wireless_cells (fixed set of ~250 cells)."""
    rows = []
    enb_counter, gnb_counter = 1000, 100_000
    for site in CELL_SITES:
        for sec_label, base_az in SECTORS:
            az = int((base_az + rng.integers(-5, 6)) % 360)
            for tech in ["4G_LTE", "5G_NR"]:
                for band_label, center_mhz, bw_mhz in TECH_BANDS[tech]:
                    cell_id = f"CELL_{site['site_id'][-2:]}_{sec_label}_{tech}_{band_label}"
                    pci = int(rng.integers(0, 504)) if tech in ("4G_LTE", "5G_NR") else None
                    tac = int(rng.integers(1000, 65535))
                    deploy = date(2020, 1, 1) + timedelta(days=int(rng.integers(0, 1400)))
                    rows.append({
                        "cell_id": cell_id,
                        "site_id": site["site_id"],
                        "site_name": site["name"],
                        "sector": sec_label,
                        "azimuth_deg": az,
                        "plmn": "001-01",  # example/test PLMN (MCC-MNC) — generic telco provider, not a real network
                        "technology": tech,
                        "band": band_label,
                        "frequency_mhz": float(center_mhz),
                        "bandwidth_mhz": float(bw_mhz),
                        "latitude": site["latitude"],
                        "longitude": site["longitude"],
                        "height_m": round(rng.uniform(18, 42), 1),
                        "downtilt_deg": round(rng.uniform(2, 10), 1),
                        "pci": pci,
                        "tac": tac,
                        "lac": None,
                        "enodeb_id": enb_counter if tech == "4G_LTE" else None,
                        "gnb_id": gnb_counter if tech == "5G_NR" else None,
                        "vendor": VENDORS[int(rng.integers(0, len(VENDORS)))],
                        "deployment_date": deploy,
                        "status": "in_service",
                    })
                    if tech == "4G_LTE":
                        enb_counter += 1
                    elif tech == "5G_NR":
                        gnb_counter += 1
    return pd.DataFrame(rows)


def _gen_wireless_connectivity(buildings: List[Dict], cells_df: pd.DataFrame,
                              rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate wireless_connectivity (per-building signal quality samples)."""
    n = int(FULL_SCALE_WIRELESS_CONN * scale)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        rows.append({
            "record_id": f"WCN_{uuid.uuid4().hex[:16]}",
            "ts": pd.Timestamp(ts),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "primary_cell_id": cells_df.iloc[i % len(cells_df)]["cell_id"],
            "technology": "4G_LTE",
            "rsrp_dbm": float(np.clip(rng.normal(-75, 10), -125, -55)),
            "rsrq_db": float(np.clip(rng.normal(-10, 5), -20, -3)),
            "sinr_db": float(np.clip(rng.normal(10, 8), -5, 30)),
            "throughput_dl_mbps": float(max(1.0, rng.normal(50, 20))),
            "throughput_ul_mbps": float(max(0.5, rng.normal(10, 5))),
            "latency_ms": float(np.clip(rng.normal(22, 6), 5, 200)),
            "handover_count_1h": int(max(0, rng.poisson(0.8))),
            "connected_devices_peak": int(max(1, 10 + b["max_floors"] * rng.uniform(2, 6))),
            "coverage_quality": "good",
        })
    return pd.DataFrame(rows)


def _gen_wireless_node_health(cells_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate wireless_node_health (per-cell hourly metrics)."""
    n_cells = len(cells_df)
    n_hours = 30 * 24
    total = n_cells * n_hours
    rows = []
    for i in range(min(total, int(total * 0.5))):  # Sample to keep reasonable size
        cell_idx = i % n_cells
        cell = cells_df.iloc[cell_idx]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        rows.append({
            "health_id": f"WNH_{uuid.uuid4().hex[:16]}",
            "ts": pd.Timestamp(ts),
            "cell_id": cell["cell_id"],
            "site_id": cell["site_id"],
            "status": rng.choice(["online", "degraded", "offline"], p=[0.97, 0.02, 0.01]),
            "prb_utilization_pct": float(np.clip(rng.normal(40, 15), 0, 100)),
            "active_users": int(max(0, rng.normal(100, 40))),
            "avg_dl_throughput_mbps": float(max(1.0, rng.normal(100, 30))),
            "avg_ul_throughput_mbps": float(max(0.5, rng.normal(20, 10))),
            "avg_latency_ms": float(np.clip(rng.normal(25, 8), 3, 200)),
            "cpu_utilization_pct": float(np.clip(rng.normal(40, 15), 5, 100)),
            "temperature_c": float(np.clip(28.0 + rng.normal(0, 3), 10, 75)),
            "alarm_code": "ALM-VSWR" if rng.random() < 0.02 else None,
            "uptime_seconds": int(rng.integers(60, 30*86400)),
        })
    return pd.DataFrame(rows)


def _gen_connectivity_presence(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate connectivity_presence (per-building Wi-Fi presence)."""
    n = int(FULL_SCALE_CONNECTIVITY * scale)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        rows.append({
            "record_id": f"CP_{b['bldg_uid']}_{i:04d}",
            "ts": pd.Timestamp(ts),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "gateway_id": f"GW_{b['bldg_uid']}_001",
            "connected_device_count": int(max(1, rng.poisson(10))),
            "unique_device_count_1h": int(max(1, rng.poisson(15))),
            "motion_detected": rng.random() < 0.2,
            "motion_events_1h": int(max(0, rng.poisson(2))),
            "wifi_signal_strength_dbm": float(np.clip(rng.normal(-55, 8), -80, -35)),
            "bandwidth_used_mbps": float(max(0.1, rng.normal(50, 30))),
        })
    return pd.DataFrame(rows)


def _gen_sc_gateway_network(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate sc_gateway_network (gateway inventory + health)."""
    n = int(FULL_SCALE_GATEWAY * scale)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        ts = WINDOW_START + rng.integers(0, WINDOW_SECONDS).astype("timedelta64[s]")
        deploy_date = (NOW - rng.integers(30, 5*365).astype("timedelta64[D]")).astype("datetime64[D]")
        rows.append({
            "gateway_id": f"GW_{b['bldg_uid']}_{(i % 10) + 1:03d}",
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "floor": None,
            "unit_id": None,
            "model": rng.choice(["XB6", "XB7", "XB8", "XB10"]),
            "firmware_version": f"v{rng.integers(1, 5)}.{rng.integers(0, 10)}.{rng.integers(0, 12)}",
            "status": rng.choice(["on", "degraded", "error", "off"], p=[0.965, 0.020, 0.010, 0.005]),
            "network_degradation_pct": float(np.clip(rng.beta(1.5, 30) * 100, 0, 100)),
            "packet_loss_pct": float(np.clip(rng.beta(1.5, 25) * 100, 0, 100)),
            "jitter_ms": float(np.clip(rng.gamma(1.5, 1.5), 0.5, 100)),
            "uptime_seconds": int(rng.integers(60, 30*86400)),
            "last_reboot_ts": pd.Timestamp(ts - rng.integers(1, 30).astype("timedelta64[D]")),
            "deployment_date": pd.Timestamp(deploy_date),
            "ts": pd.Timestamp(ts),
        })
    return pd.DataFrame(rows)


def _gen_udl_campus_metrics(buildings: List[Dict], rng: np.random.Generator, scale: float) -> pd.DataFrame:
    """Generate udl_campus_metrics (daily rollups)."""
    n = int(FULL_SCALE_UDL * scale)
    rows = []
    for i in range(n):
        b = buildings[i % len(buildings)]
        date_val = (NOW - rng.integers(0, 30).astype("timedelta64[D]")).astype("datetime64[D]")
        rows.append({
            "record_id": f"UDL_{uuid.uuid4().hex[:16]}",
            "date": pd.Timestamp(date_val).date(),
            "bldg_uid": b["bldg_uid"],
            "building_name": b["building_name"],
            "category": b["category"],
            "legacy_building_id": None,
            "occupancy_pct": float(np.clip(rng.normal(60, 20), 0, 100)),
            "peak_occupancy_count": int(max(1, rng.normal(100, 30))),
            "hvac_load_kwh": float(max(0.1, rng.normal(500, 150))),
            "energy_consumption_kwh": float(max(0.1, rng.normal(2000, 500))),
            "water_consumption_m3": float(max(0.01, rng.normal(50, 20))),
            "ev_sessions_count": int(max(0, rng.poisson(2))),
            "ev_energy_delivered_kwh": float(max(0, rng.normal(30, 15))),
            "avg_air_quality_index": int(np.clip(rng.normal(50, 20), 0, 500)),
            "avg_pm25": float(np.clip(rng.normal(15, 8), 0, 300)),
            "avg_temperature_c": float(np.clip(rng.normal(12, 4), 0, 40)),
            "avg_humidity_pct": float(np.clip(rng.normal(65, 15), 0, 100)),
            "pedestrian_count_daily": int(max(0, rng.normal(500, 200))),
            "vehicle_count_daily": int(max(0, rng.normal(300, 100))),
            "safety_incidents_count": int(max(0, rng.poisson(0.5))),
            "safety_incident_types": None,
        })
    return pd.DataFrame(rows)


# ============================================================================
# Schema Definitions
# ============================================================================

def _schema_dim_buildings() -> StructType:
    return StructType([
        StructField("building_id", StringType(), False),
        StructField("bcom_uid", StringType(), True),
        StructField("building_name", StringType(), False),
        StructField("address", StringType(), True),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("n_units", LongType(), False),
        StructField("complex_type", StringType(), False),
        StructField("year_built", LongType(), False),
    ])


def _schema_dim_zones() -> StructType:
    return StructType([
        StructField("zone_id", StringType(), False),
        StructField("zone_name", StringType(), False),
        StructField("zone_type", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
    ])


def _schema_sc_access_events() -> StructType:
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("unit_id", StringType(), True),
        StructField("device_id", StringType(), False),
        StructField("occupant_id", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("door_state", StringType(), False),
        StructField("credential_type", StringType(), False),
        StructField("success", BooleanType(), False),
        StructField("retries", IntegerType(), False),
        StructField("address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("floor", IntegerType(), True),
    ])


def _schema_sc_device_health() -> StructType:
    return StructType([
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("ts", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("firmware_version", StringType(), False),
        StructField("uptime_seconds", LongType(), False),
        StructField("fault_code", StringType(), True),
        StructField("last_reboot_ts", TimestampType(), False),
        StructField("packet_loss_pct", DoubleType(), False),
        StructField("temperature_c", DoubleType(), False),
    ])


def _schema_mobility_presence() -> StructType:
    return StructType([
        StructField("record_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("anon_device_id", StringType(), False),
        StructField("source_cell_id", StringType(), False),
        StructField("dwell_seconds", IntegerType(), False),
        StructField("device_count_in_building", IntegerType(), False),
        StructField("signal_strength_dbm", DoubleType(), False),
        StructField("arrival_flag", BooleanType(), False),
        StructField("departure_flag", BooleanType(), False),
        StructField("movement_type", StringType(), False),
    ])


def _schema_wireless_cells() -> StructType:
    return StructType([
        StructField("cell_id", StringType(), False),
        StructField("site_id", StringType(), False),
        StructField("site_name", StringType(), False),
        StructField("sector", StringType(), False),
        StructField("azimuth_deg", IntegerType(), False),
        StructField("plmn", StringType(), False),
        StructField("technology", StringType(), False),
        StructField("band", StringType(), False),
        StructField("frequency_mhz", DoubleType(), False),
        StructField("bandwidth_mhz", DoubleType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("height_m", DoubleType(), False),
        StructField("downtilt_deg", DoubleType(), False),
        StructField("pci", IntegerType(), True),
        StructField("tac", IntegerType(), True),
        StructField("lac", IntegerType(), True),
        StructField("enodeb_id", IntegerType(), True),
        StructField("gnb_id", IntegerType(), True),
        StructField("vendor", StringType(), False),
        StructField("deployment_date", DateType(), False),
        StructField("status", StringType(), False),
    ])


def _schema_wireless_connectivity() -> StructType:
    return StructType([
        StructField("record_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("primary_cell_id", StringType(), False),
        StructField("technology", StringType(), False),
        StructField("rsrp_dbm", DoubleType(), False),
        StructField("rsrq_db", DoubleType(), False),
        StructField("sinr_db", DoubleType(), False),
        StructField("throughput_dl_mbps", DoubleType(), False),
        StructField("throughput_ul_mbps", DoubleType(), False),
        StructField("latency_ms", DoubleType(), False),
        StructField("handover_count_1h", IntegerType(), False),
        StructField("connected_devices_peak", IntegerType(), False),
        StructField("coverage_quality", StringType(), False),
    ])


def _schema_wireless_node_health() -> StructType:
    return StructType([
        StructField("health_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("cell_id", StringType(), False),
        StructField("site_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("prb_utilization_pct", DoubleType(), False),
        StructField("active_users", IntegerType(), False),
        StructField("avg_dl_throughput_mbps", DoubleType(), False),
        StructField("avg_ul_throughput_mbps", DoubleType(), False),
        StructField("avg_latency_ms", DoubleType(), False),
        StructField("cpu_utilization_pct", DoubleType(), False),
        StructField("temperature_c", DoubleType(), False),
        StructField("alarm_code", StringType(), True),
        StructField("uptime_seconds", LongType(), False),
    ])


def _schema_connectivity_presence() -> StructType:
    return StructType([
        StructField("record_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("gateway_id", StringType(), False),
        StructField("connected_device_count", IntegerType(), False),
        StructField("unique_device_count_1h", IntegerType(), False),
        StructField("motion_detected", BooleanType(), False),
        StructField("motion_events_1h", IntegerType(), False),
        StructField("wifi_signal_strength_dbm", DoubleType(), False),
        StructField("bandwidth_used_mbps", DoubleType(), False),
    ])


def _schema_sc_gateway_network() -> StructType:
    return StructType([
        StructField("gateway_id", StringType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("unit_id", StringType(), True),
        StructField("model", StringType(), False),
        StructField("firmware_version", StringType(), False),
        StructField("status", StringType(), False),
        StructField("network_degradation_pct", DoubleType(), False),
        StructField("packet_loss_pct", DoubleType(), False),
        StructField("jitter_ms", DoubleType(), False),
        StructField("uptime_seconds", LongType(), False),
        StructField("last_reboot_ts", TimestampType(), False),
        StructField("deployment_date", DateType(), False),
        StructField("ts", TimestampType(), False),
    ])


def _schema_udl_campus_metrics() -> StructType:
    return StructType([
        StructField("record_id", StringType(), False),
        StructField("date", DateType(), False),
        StructField("bldg_uid", StringType(), False),
        StructField("building_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("legacy_building_id", StringType(), True),
        StructField("occupancy_pct", DoubleType(), False),
        StructField("peak_occupancy_count", IntegerType(), False),
        StructField("hvac_load_kwh", DoubleType(), False),
        StructField("energy_consumption_kwh", DoubleType(), False),
        StructField("water_consumption_m3", DoubleType(), False),
        StructField("ev_sessions_count", IntegerType(), False),
        StructField("ev_energy_delivered_kwh", DoubleType(), False),
        StructField("avg_air_quality_index", DoubleType(), False),
        StructField("avg_pm25", DoubleType(), False),
        StructField("avg_temperature_c", DoubleType(), False),
        StructField("avg_humidity_pct", DoubleType(), False),
        StructField("pedestrian_count_daily", IntegerType(), False),
        StructField("vehicle_count_daily", IntegerType(), False),
        StructField("safety_incidents_count", IntegerType(), False),
        StructField("safety_incident_types", StringType(), True),
    ])


# ============================================================================
# Default Residences (if GeoJSON unavailable)
# ============================================================================

def _default_residences() -> List[Dict]:
    """Fallback residences if ubcv_complexes.geojson unavailable."""
    return [
        {"bcom_uid": "VBC10018", "name": "Walter H. Gage Residence", "usage": "StudentHousing",
         "gba": 46115, "latitude": 49.26972, "longitude": -123.249256, "estimated_units": 615},
        {"bcom_uid": "VBC10011", "name": "Totem Park Residence", "usage": "StudentHousing",
         "gba": 83656, "latitude": 49.2585, "longitude": -123.252489, "estimated_units": 1115},
        {"bcom_uid": "VBC10019", "name": "Place Vanier Residence", "usage": "StudentHousing",
         "gba": 35552, "latitude": 49.264456, "longitude": -123.258878, "estimated_units": 474},
        {"bcom_uid": "VBC10023", "name": "Ponderosa Commons", "usage": "Commons",
         "gba": 55655, "latitude": 49.263797, "longitude": -123.255291, "estimated_units": 742},
        {"bcom_uid": "VBC10017", "name": "Marine Drive Residence", "usage": "StudentHousing",
         "gba": 59324, "latitude": 49.261477, "longitude": -123.255981, "estimated_units": 791},
        {"bcom_uid": "VBC10021", "name": "Brock Commons", "usage": "Commons",
         "gba": 46569, "latitude": 49.26931, "longitude": -123.252893, "estimated_units": 621},
        {"bcom_uid": "VBC10010", "name": "Thunderbird Residence", "usage": "StudentHousing",
         "gba": 42208, "latitude": 49.259615, "longitude": -123.248931, "estimated_units": 563},
        {"bcom_uid": "VBC10030", "name": "Fairview Crescent Residence", "usage": "StudentHousing",
         "gba": 20070, "latitude": 49.263197, "longitude": -123.239722, "estimated_units": 268},
    ] + [
        {"bcom_uid": f"VBC_OTHER_{i}", "name": f"Building {i}", "usage": "Housing",
         "gba": 10000 + i*100, "latitude": 49.25 + i*0.001, "longitude": -123.24 - i*0.001,
         "estimated_units": 100 + i*10}
        for i in range(30)
    ]
