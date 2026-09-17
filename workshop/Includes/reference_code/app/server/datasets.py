"""Per-dataset query builders.

Each dataset returns a dict:
  {
    "markers": [{id, name, latitude, longitude, metric, metric_label, details}],
    "total_rows": int,
    "unique_entities": int,
    "aggregates": {...},
    "preview": [first 10 rows]
  }
"""
from __future__ import annotations
from typing import Dict, Any, Tuple
from .config import table
from .sql import run_query


# Time range clause builder -------------------------------------------------
# The dataset was generated with event times up to mid-April 2026. To make the
# "last 24h / 7d / 30d" filter meaningful against that historical window, we
# anchor the filter to the max timestamp in each table rather than NOW().

def _time_where(ts_col: str, table_name: str, range_key: str) -> str:
    if range_key == "all":
        return ""
    days_map = {"24h": 1, "7d": 7, "30d": 30}
    days = days_map.get(range_key)
    if not days:
        return ""
    return (
        f"WHERE {ts_col} >= (SELECT MAX({ts_col}) - INTERVAL {days} DAYS "
        f"FROM {table_name})"
    )


# -------------------- dim_buildings --------------------
def dim_buildings(_range: str) -> Dict[str, Any]:
    rows = run_query(f"""
        SELECT bldg_uid, building_name, building_code, address, category,
               neighbourhood, latitude, longitude
        FROM {table('dim_buildings')}
        ORDER BY building_name
    """)
    markers = [
        {
            "id": r["bldg_uid"],
            "name": r["building_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": 1,
            "metric_label": r["category"] or "Building",
            "details": r,
        }
        for r in rows
    ]

    def _distinct(key: str) -> int:
        return len({r[key] for r in rows if r.get(key)})

    return {
        "markers": markers,
        "total_rows": len(rows),
        "unique_entities": len(rows),
        "aggregates": {
            "categories": _distinct("category"),
            "neighbourhoods": _distinct("neighbourhood"),
        },
        "preview": rows[:10],
    }


# -------------------- dim_zones --------------------
def dim_zones(_range: str) -> Dict[str, Any]:
    rows = run_query(f"""
        SELECT zone_id, zone_name, zone_type, latitude, longitude
        FROM {table('dim_zones')}
        ORDER BY zone_name
    """)
    markers = [
        {
            "id": r["zone_id"],
            "name": r["zone_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": 1,
            "metric_label": r["zone_type"],
            "details": r,
        }
        for r in rows
    ]
    type_counts: Dict[str, int] = {}
    for r in rows:
        type_counts[r["zone_type"]] = type_counts.get(r["zone_type"], 0) + 1
    return {
        "markers": markers,
        "total_rows": len(rows),
        "unique_entities": len(rows),
        "aggregates": {"zone_types": type_counts},
        "preview": rows[:10],
    }


# -------------------- sc_access_events --------------------
def sc_access_events(range_key: str) -> Dict[str, Any]:
    tbl = table("sc_access_events")
    where = _time_where("event_ts", tbl, range_key)
    agg_rows = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name) building_name,
               ANY_VALUE(category) category,
               ANY_VALUE(legacy_building_id) legacy_building_id,
               ANY_VALUE(address) address,
               ANY_VALUE(latitude) latitude,
               ANY_VALUE(longitude) longitude,
               COUNT(*) event_count,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) success_count,
               SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) fail_count,
               COUNT(DISTINCT occupant_id) unique_occupants,
               COUNT(DISTINCT device_id) device_count
        FROM {tbl}
        {where}
        GROUP BY bldg_uid
        ORDER BY event_count DESC
    """)
    total = sum(r["event_count"] for r in agg_rows)
    total_fail = sum(r["fail_count"] or 0 for r in agg_rows)
    markers = [
        {
            "id": r["bldg_uid"],
            "name": r["building_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": r["event_count"],
            "metric_label": f"{r['event_count']:,} events",
            "details": {
                "bldg_uid": r["bldg_uid"],
                "legacy_building_id": r.get("legacy_building_id"),
                "category": r.get("category"),
                "building_name": r["building_name"],
                "address": r["address"],
                "event_count": r["event_count"],
                "success_count": r["success_count"],
                "fail_count": r["fail_count"],
                "unique_occupants": r["unique_occupants"],
                "device_count": r["device_count"],
                "success_rate_pct": round(100.0 * (r["success_count"] or 0) / max(r["event_count"], 1), 2),
            },
        }
        for r in agg_rows
    ]
    preview = run_query(f"""
        SELECT event_ts, bldg_uid, building_name, category, event_type, door_state, credential_type, success
        FROM {tbl}
        {where}
        ORDER BY event_ts DESC
        LIMIT 10
    """)
    return {
        "markers": markers,
        "total_rows": total,
        "unique_entities": len(agg_rows),
        "aggregates": {
            "total_events": total,
            "failed_events": total_fail,
            "success_rate_pct": round(100.0 * (total - total_fail) / max(total, 1), 2),
            "buildings_covered": len(agg_rows),
        },
        "preview": preview,
    }


# -------------------- sc_device_health --------------------
def sc_device_health(range_key: str) -> Dict[str, Any]:
    tbl = table("sc_device_health")
    atbl = table("sc_access_events")
    where = _time_where("ts", tbl, range_key)
    # sc_device_health no longer carries latitude/longitude/address; pull those
    # per bldg_uid from sc_access_events (same bldg_uid set).
    # Recompute the where clause scoped to the `h` alias to avoid ambiguity.
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE h.ts >= (SELECT MAX(ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    agg = run_query(f"""
        WITH geo AS (
          SELECT bldg_uid,
                 ANY_VALUE(latitude)  AS latitude,
                 ANY_VALUE(longitude) AS longitude,
                 ANY_VALUE(address)   AS address
          FROM {atbl} GROUP BY bldg_uid
        )
        SELECT h.bldg_uid,
               ANY_VALUE(h.building_name) building_name,
               ANY_VALUE(h.category) category,
               ANY_VALUE(h.legacy_building_id) legacy_building_id,
               ANY_VALUE(g.address) address,
               ANY_VALUE(g.latitude) latitude,
               ANY_VALUE(g.longitude) longitude,
               COUNT(DISTINCT h.device_id) device_count,
               COUNT(*) telemetry_count,
               SUM(CASE WHEN h.status = 'online' THEN 1 ELSE 0 END) online_count,
               SUM(CASE WHEN h.status != 'online' THEN 1 ELSE 0 END) offline_count,
               AVG(h.uptime_seconds) avg_uptime,
               COUNT(DISTINCT h.fault_code) distinct_faults
        FROM {tbl} h
        LEFT JOIN geo g ON g.bldg_uid = h.bldg_uid
        {where_clause}
        GROUP BY h.bldg_uid
        ORDER BY device_count DESC
    """)
    markers = [
        {
            "id": r["bldg_uid"],
            "name": r["building_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": r["device_count"],
            "metric_label": f"{r['device_count']} devices",
            "details": {
                "bldg_uid": r["bldg_uid"],
                "category": r.get("category"),
                "legacy_building_id": r.get("legacy_building_id"),
                "building_name": r["building_name"],
                "device_count": r["device_count"],
                "telemetry_count": r["telemetry_count"],
                "online_pings": r["online_count"],
                "offline_pings": r["offline_count"],
                "avg_uptime_hours": round((r["avg_uptime"] or 0) / 3600.0, 1),
                "distinct_faults": r["distinct_faults"],
            },
        }
        for r in agg
    ]
    preview = run_query(f"""
        SELECT ts, device_id, device_type, bldg_uid, building_name, category, status, fault_code, uptime_seconds
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total_tel = sum(r["telemetry_count"] for r in agg)
    return {
        "markers": markers,
        "total_rows": total_tel,
        "unique_entities": sum(r["device_count"] for r in agg),
        "aggregates": {
            "total_telemetry_pings": total_tel,
            "total_devices": sum(r["device_count"] for r in agg),
            "online_pings": sum(r["online_count"] for r in agg),
            "offline_pings": sum(r["offline_count"] for r in agg),
        },
        "preview": preview,
    }


# -------------------- mobility_presence (building-based) --------------------
def mobility_presence(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation of the new building-anchored mobility_presence."""
    tbl = table("mobility_presence")
    atbl = table("sc_access_events")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE m.ts >= (SELECT MAX(ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    agg = run_query(f"""
        WITH geo AS (
          SELECT bldg_uid,
                 ANY_VALUE(latitude)  AS latitude,
                 ANY_VALUE(longitude) AS longitude
          FROM {atbl} GROUP BY bldg_uid
        )
        SELECT m.bldg_uid,
               ANY_VALUE(m.building_name)        AS building_name,
               ANY_VALUE(m.category)             AS category,
               ANY_VALUE(m.legacy_building_id)   AS legacy_building_id,
               ANY_VALUE(g.latitude)             AS latitude,
               ANY_VALUE(g.longitude)            AS longitude,
               COUNT(*)                          AS record_count,
               COUNT(DISTINCT m.anon_device_id)  AS unique_devices,
               AVG(m.dwell_seconds)              AS avg_dwell,
               AVG(m.device_count_in_building)   AS avg_device_count,
               MAX(m.device_count_in_building)   AS peak_concurrent,
               AVG(m.signal_strength_dbm)        AS avg_signal,
               SUM(CASE WHEN m.arrival_flag   THEN 1 ELSE 0 END) AS arrivals,
               SUM(CASE WHEN m.departure_flag THEN 1 ELSE 0 END) AS departures
        FROM {tbl} m
        LEFT JOIN geo g ON g.bldg_uid = m.bldg_uid
        {where_clause}
        GROUP BY m.bldg_uid
        ORDER BY record_count DESC
    """)
    markers = [
        {
            "id": r["bldg_uid"],
            "name": r["building_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": r["unique_devices"],
            "metric_label": f"{r['unique_devices']:,} devices",
            "details": {
                "bldg_uid": r["bldg_uid"],
                "category": r.get("category"),
                "building_name": r["building_name"],
                "record_count": r["record_count"],
                "unique_devices": r["unique_devices"],
                "avg_dwell_seconds": round(r["avg_dwell"] or 0, 1),
                "avg_device_count": round(r["avg_device_count"] or 0, 1),
                "peak_concurrent": r["peak_concurrent"],
                "avg_signal_dbm": round(r["avg_signal"] or 0, 1),
                "arrivals": r["arrivals"],
                "departures": r["departures"],
            },
        }
        for r in agg
    ]
    preview_where = (
        f"WHERE ts >= (SELECT MAX(ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    preview = run_query(f"""
        SELECT ts, bldg_uid, building_name, category, anon_device_id, source_cell_id,
               dwell_seconds, device_count_in_building, signal_strength_dbm, movement_type
        FROM {tbl}
        {preview_where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total = sum(r["record_count"] for r in agg)
    return {
        "markers": markers,
        "total_rows": total,
        "unique_entities": len(agg),
        "aggregates": {
            "total_presence_records": total,
            "total_unique_devices": sum(r["unique_devices"] for r in agg),
            "buildings_active": len(agg),
        },
        "preview": preview,
    }


# -------------------- wireless_cells (direct, no aggregation) --------------------
def wireless_cells(range_key: str) -> Dict[str, Any]:
    """Return all cell records (one per sector/tech/band) with lat/lon."""
    tbl = table("wireless_cells")
    rows = run_query(f"""
        SELECT cell_id, site_id, site_name, sector, azimuth_deg, plmn, technology,
               band, frequency_mhz, bandwidth_mhz, latitude, longitude, height_m,
               downtilt_deg, pci, tac, lac, enodeb_id, gnb_id, vendor,
               deployment_date, status
        FROM {tbl}
        ORDER BY site_id, sector, technology
    """)
    tech_colors = {
        "2G_GSM":  "#9CA3AF",
        "3G_UMTS": "#3B82F6",
        "4G_LTE":  "#10B981",
        "5G_NR":   "#8B5CF6",
    }
    markers = [
        {
            "id": r["cell_id"],
            "name": r["site_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": 1,
            "metric_label": f"{r['technology']} {r['band']}",
            "color": tech_colors.get(r["technology"], "#6b7280"),
            "details": {
                "cell_id": r["cell_id"],
                "site_name": r["site_name"],
                "sector": r["sector"],
                "azimuth_deg": r["azimuth_deg"],
                "technology": r["technology"],
                "band": r["band"],
                "frequency_mhz": r["frequency_mhz"],
                "bandwidth_mhz": r["bandwidth_mhz"],
                "plmn": r["plmn"],
                "pci": r["pci"],
                "tac": r["tac"],
                "enodeb_id": r["enodeb_id"],
                "gnb_id": r["gnb_id"],
                "vendor": r["vendor"],
                "height_m": r["height_m"],
                "status": r["status"],
            },
        }
        for r in rows
    ]
    # Aggregates
    tech_counts: Dict[str, int] = {}
    site_counts: Dict[str, int] = {}
    status_counts: Dict[str, int] = {}
    for r in rows:
        tech_counts[r["technology"]] = tech_counts.get(r["technology"], 0) + 1
        site_counts[r["site_id"]] = site_counts.get(r["site_id"], 0) + 1
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    return {
        "markers": markers,
        "total_rows": len(rows),
        "unique_entities": len(site_counts),
        "aggregates": {
            "total_cells": len(rows),
            "total_sites": len(site_counts),
            "by_technology": tech_counts,
            "by_status": status_counts,
        },
        "preview": rows[:10],
    }


# -------------------- wireless_connectivity --------------------
def wireless_connectivity(range_key: str) -> Dict[str, Any]:
    """Summary shown in the right-hand panel. The map heatmap uses the
    dedicated /api/telemetry/wireless_connectivity/buildings endpoint."""
    tbl = table("wireless_connectivity")
    where = _time_where("ts", tbl, range_key)
    agg = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name) building_name,
               ANY_VALUE(category) category,
               COUNT(*) record_count,
               AVG(rsrp_dbm) avg_rsrp,
               AVG(sinr_db) avg_sinr,
               AVG(throughput_dl_mbps) avg_dl,
               AVG(throughput_ul_mbps) avg_ul,
               AVG(latency_ms) avg_latency,
               MAX(connected_devices_peak) peak_devices
        FROM {tbl}
        {where}
        GROUP BY bldg_uid
        ORDER BY avg_rsrp DESC
    """)
    preview = run_query(f"""
        SELECT ts, bldg_uid, building_name, technology, rsrp_dbm, sinr_db,
               throughput_dl_mbps, coverage_quality
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total_rec = sum(r["record_count"] for r in agg)
    avg_rsrp_all = (
        sum((r["avg_rsrp"] or 0) * r["record_count"] for r in agg) / max(total_rec, 1)
    )
    avg_dl_all = (
        sum((r["avg_dl"] or 0) * r["record_count"] for r in agg) / max(total_rec, 1)
    )
    return {
        "markers": [],  # map is driven by /buildings endpoint
        "total_rows": total_rec,
        "unique_entities": len(agg),
        "aggregates": {
            "total_records": total_rec,
            "buildings_covered": len(agg),
            "campus_avg_rsrp_dbm": round(avg_rsrp_all, 2),
            "campus_avg_dl_mbps": round(avg_dl_all, 2),
        },
        "preview": preview,
    }


# -------------------- wireless_node_health --------------------
def wireless_node_health(range_key: str) -> Dict[str, Any]:
    """Summary shown in the right-hand panel. The map uses
    /api/telemetry/wireless_node_health/cells for per-cell markers."""
    tbl = table("wireless_node_health")
    where = _time_where("ts", tbl, range_key)
    agg = run_query(f"""
        SELECT cell_id,
               ANY_VALUE(site_id) site_id,
               COUNT(*) record_count,
               SUM(CASE WHEN status='online'      THEN 1 ELSE 0 END) online_pings,
               SUM(CASE WHEN status='degraded'    THEN 1 ELSE 0 END) degraded_pings,
               SUM(CASE WHEN status='offline'     THEN 1 ELSE 0 END) offline_pings,
               SUM(CASE WHEN status='maintenance' THEN 1 ELSE 0 END) maintenance_pings,
               AVG(prb_utilization_pct) avg_prb,
               AVG(active_users) avg_users,
               AVG(avg_dl_throughput_mbps) avg_dl,
               AVG(avg_latency_ms) avg_latency,
               SUM(CASE WHEN alarm_code IS NOT NULL THEN 1 ELSE 0 END) alarm_count
        FROM {tbl}
        {where}
        GROUP BY cell_id
        ORDER BY record_count DESC
    """)
    preview = run_query(f"""
        SELECT ts, cell_id, site_id, status, prb_utilization_pct, active_users,
               avg_latency_ms, alarm_code
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total_rec = sum(r["record_count"] for r in agg)
    total_alarms = sum(r["alarm_count"] or 0 for r in agg)
    status_totals = {
        "online": sum(r["online_pings"] or 0 for r in agg),
        "degraded": sum(r["degraded_pings"] or 0 for r in agg),
        "offline": sum(r["offline_pings"] or 0 for r in agg),
        "maintenance": sum(r["maintenance_pings"] or 0 for r in agg),
    }
    return {
        "markers": [],  # map is driven by /cells endpoint
        "total_rows": total_rec,
        "unique_entities": len(agg),
        "aggregates": {
            "total_records": total_rec,
            "total_cells": len(agg),
            "total_alarms": total_alarms,
            "status_totals": status_totals,
        },
        "preview": preview,
    }


# -------------------- gateway_telemetry --------------------
def gateway_telemetry(range_key: str) -> Dict[str, Any]:
    tbl = table("gateway_telemetry")
    where = _time_where("ts", tbl, range_key)
    agg = run_query(f"""
        SELECT building_id,
               ANY_VALUE(building_name) building_name,
               ANY_VALUE(latitude) latitude,
               ANY_VALUE(longitude) longitude,
               COUNT(DISTINCT gateway_id) gateway_count,
               COUNT(*) telemetry_count,
               AVG(connected_device_count) avg_connected,
               AVG(wifi_signal_strength) avg_wifi,
               AVG(packet_loss_pct) avg_packet_loss,
               AVG(jitter_ms) avg_jitter,
               SUM(CASE WHEN reboot_flag THEN 1 ELSE 0 END) reboot_count
        FROM {tbl}
        {where}
        GROUP BY building_id
        ORDER BY gateway_count DESC
    """)
    markers = [
        {
            "id": r["building_id"],
            "name": r["building_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": r["gateway_count"],
            "metric_label": f"{r['gateway_count']} gateways",
            "details": {
                "building_name": r["building_name"],
                "gateway_count": r["gateway_count"],
                "telemetry_count": r["telemetry_count"],
                "avg_connected_devices": round(r["avg_connected"] or 0, 1),
                "avg_wifi_signal_dbm": round(r["avg_wifi"] or 0, 1),
                "avg_packet_loss_pct": round(r["avg_packet_loss"] or 0, 3),
                "avg_jitter_ms": round(r["avg_jitter"] or 0, 2),
                "reboot_events": r["reboot_count"],
            },
        }
        for r in agg
    ]
    preview = run_query(f"""
        SELECT ts, gateway_id, building_name, connected_device_count,
               wifi_signal_strength, packet_loss_pct, reboot_flag
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total = sum(r["telemetry_count"] for r in agg)
    return {
        "markers": markers,
        "total_rows": total,
        "unique_entities": sum(r["gateway_count"] for r in agg),
        "aggregates": {
            "total_telemetry_records": total,
            "total_gateways": sum(r["gateway_count"] for r in agg),
            "total_reboots": sum(r["reboot_count"] for r in agg),
            "buildings_covered": len(agg),
        },
        "preview": preview,
    }


# -------------------- udl_ev_charging --------------------
def udl_ev_charging(range_key: str) -> Dict[str, Any]:
    tbl = table("udl_ev_charging")
    where = _time_where("start_ts", tbl, range_key)
    agg = run_query(f"""
        SELECT station_id,
               ANY_VALUE(location) location,
               ANY_VALUE(latitude) latitude,
               ANY_VALUE(longitude) longitude,
               COUNT(*) session_count,
               SUM(energy_kwh) total_kwh,
               AVG(energy_kwh) avg_kwh,
               AVG(duration_minutes) avg_duration,
               SUM(cost_cad) total_revenue,
               COUNT(DISTINCT user_anon_id) unique_users
        FROM {tbl}
        {where}
        GROUP BY station_id
        ORDER BY total_kwh DESC
    """)
    markers = [
        {
            "id": r["station_id"],
            "name": r["location"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": round(r["total_kwh"] or 0, 1),
            "metric_label": f"{round(r['total_kwh'] or 0, 1):,} kWh",
            "details": {
                "location": r["location"],
                "session_count": r["session_count"],
                "total_kwh": round(r["total_kwh"] or 0, 2),
                "avg_kwh_per_session": round(r["avg_kwh"] or 0, 2),
                "avg_duration_min": round(r["avg_duration"] or 0, 1),
                "total_revenue_cad": round(r["total_revenue"] or 0, 2),
                "unique_users": r["unique_users"],
            },
        }
        for r in agg
    ]
    preview = run_query(f"""
        SELECT start_ts, station_id, location, energy_kwh, duration_minutes, cost_cad
        FROM {tbl}
        {where}
        ORDER BY start_ts DESC
        LIMIT 10
    """)
    total_sess = sum(r["session_count"] for r in agg)
    return {
        "markers": markers,
        "total_rows": total_sess,
        "unique_entities": len(agg),
        "aggregates": {
            "total_sessions": total_sess,
            "total_kwh": round(sum(r["total_kwh"] or 0 for r in agg), 1),
            "total_revenue_cad": round(sum(r["total_revenue"] or 0 for r in agg), 2),
            "total_stations": len(agg),
        },
        "preview": preview,
    }


# -------------------- udl_environmental --------------------
def udl_environmental(range_key: str) -> Dict[str, Any]:
    tbl = table("udl_environmental")
    where = _time_where("ts", tbl, range_key)
    agg = run_query(f"""
        SELECT sensor_id,
               ANY_VALUE(location) location,
               ANY_VALUE(latitude) latitude,
               ANY_VALUE(longitude) longitude,
               COUNT(*) reading_count,
               AVG(temperature_c) avg_temp,
               AVG(humidity_pct) avg_humidity,
               AVG(air_quality_index) avg_aqi,
               AVG(pm25) avg_pm25,
               AVG(co2_ppm) avg_co2,
               AVG(wind_speed_mps) avg_wind
        FROM {tbl}
        {where}
        GROUP BY sensor_id
        ORDER BY reading_count DESC
    """)
    markers = [
        {
            "id": r["sensor_id"],
            "name": r["location"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "metric": round(r["avg_temp"] or 0, 1),
            "metric_label": f"{round(r['avg_temp'] or 0, 1)}°C",
            "details": {
                "location": r["location"],
                "reading_count": r["reading_count"],
                "avg_temp_c": round(r["avg_temp"] or 0, 2),
                "avg_humidity_pct": round(r["avg_humidity"] or 0, 1),
                "avg_aqi": round(r["avg_aqi"] or 0, 1),
                "avg_pm25": round(r["avg_pm25"] or 0, 2),
                "avg_co2_ppm": round(r["avg_co2"] or 0, 1),
                "avg_wind_mps": round(r["avg_wind"] or 0, 2),
            },
        }
        for r in agg
    ]
    preview = run_query(f"""
        SELECT ts, sensor_id, location, temperature_c, humidity_pct,
               air_quality_index, pm25, co2_ppm
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total = sum(r["reading_count"] for r in agg)
    return {
        "markers": markers,
        "total_rows": total,
        "unique_entities": len(agg),
        "aggregates": {
            "total_readings": total,
            "total_sensors": len(agg),
            "overall_avg_temp_c": round(
                sum((r["avg_temp"] or 0) * r["reading_count"] for r in agg) / max(total, 1), 2
            ),
            "overall_avg_aqi": round(
                sum((r["avg_aqi"] or 0) * r["reading_count"] for r in agg) / max(total, 1), 1
            ),
        },
        "preview": preview,
    }


# -------------------- device_health per-building aggregation (heatmap) --------------------
def device_health_buildings(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation of sc_device_health for the heatmap visualization."""
    tbl = table("sc_device_health")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE ts >= (SELECT MAX(ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    rows = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name)      AS building_name,
               ANY_VALUE(category)           AS category,
               ANY_VALUE(legacy_building_id) AS legacy_building_id,
               COUNT(*)                      AS telemetry_count,
               COUNT(DISTINCT device_id)     AS device_count,
               SUM(CASE WHEN status='online'   THEN 1 ELSE 0 END) AS online_pings,
               SUM(CASE WHEN status='offline'  THEN 1 ELSE 0 END) AS offline_pings,
               SUM(CASE WHEN status='degraded' THEN 1 ELSE 0 END) AS degraded_pings,
               ROUND(AVG(uptime_seconds)/3600.0, 1) AS avg_uptime_hours,
               COUNT(DISTINCT CASE WHEN fault_code IS NOT NULL THEN fault_code END) AS distinct_faults
        FROM {tbl}
        {where_clause}
        GROUP BY bldg_uid
        ORDER BY telemetry_count DESC
    """)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- access_events per-building aggregation (heatmap) --------------------
def access_events_buildings(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation of sc_access_events for the heatmap visualization."""
    tbl = table("sc_access_events")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE event_ts >= (SELECT MAX(event_ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    rows = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name)      AS building_name,
               ANY_VALUE(category)           AS category,
               ANY_VALUE(legacy_building_id) AS legacy_building_id,
               COUNT(*)                      AS event_count,
               COUNT(DISTINCT device_id)     AS device_count,
               COUNT(DISTINCT occupant_id)   AS unique_occupants,
               SUM(CASE WHEN event_type='door_open'         THEN 1 ELSE 0 END) AS door_opens,
               SUM(CASE WHEN event_type='door_forced'       THEN 1 ELSE 0 END) AS door_forced,
               SUM(CASE WHEN event_type IN ('intercom_call','intercom_answered','intercom_missed') THEN 1 ELSE 0 END) AS intercom_calls,
               SUM(CASE WHEN event_type='credential_fail'   THEN 1 ELSE 0 END) AS credential_failures,
               SUM(CASE WHEN event_type='tailgate_detected' THEN 1 ELSE 0 END) AS tailgate_events,
               ROUND(100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
        FROM {tbl}
        {where_clause}
        GROUP BY bldg_uid
        ORDER BY event_count DESC
    """)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- wireless_connectivity per-building aggregation --------------------
def wireless_connectivity_buildings(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation for the connectivity heatmap."""
    tbl = table("wireless_connectivity")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    cutoff_cte = (
        f"cutoff AS (SELECT MAX(ts) - INTERVAL {_days} DAYS AS ts_min FROM {tbl}),"
        if _days else ""
    )
    cutoff_where = "WHERE ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    cutoff_where_c = "WHERE c.ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    rows = run_query(f"""
        WITH {cutoff_cte}
        tech_counts AS (
          SELECT bldg_uid, technology, COUNT(*) as n
          FROM {tbl}
          {cutoff_where}
          GROUP BY bldg_uid, technology
        ),
        dominant AS (
          SELECT bldg_uid, technology,
                 ROW_NUMBER() OVER (PARTITION BY bldg_uid ORDER BY n DESC) AS rn
          FROM tech_counts
        )
        SELECT c.bldg_uid,
               ANY_VALUE(c.building_name)              AS building_name,
               ANY_VALUE(c.category)                   AS category,
               ANY_VALUE(c.primary_cell_id)            AS primary_cell_id,
               ANY_VALUE(d.technology)                 AS dominant_technology,
               COUNT(*)                                AS record_count,
               ROUND(AVG(c.rsrp_dbm), 2)               AS avg_rsrp,
               ROUND(AVG(c.rsrq_db), 2)                AS avg_rsrq,
               ROUND(AVG(c.sinr_db), 2)                AS avg_sinr,
               ROUND(AVG(c.throughput_dl_mbps), 2)     AS avg_dl_mbps,
               ROUND(AVG(c.throughput_ul_mbps), 2)     AS avg_ul_mbps,
               ROUND(AVG(c.latency_ms), 2)             AS avg_latency_ms,
               ROUND(AVG(c.connected_devices_peak), 1) AS avg_connected_peak,
               MAX(c.connected_devices_peak)           AS max_connected_peak
        FROM {tbl} c
        LEFT JOIN dominant d ON d.bldg_uid = c.bldg_uid AND d.rn = 1
        {cutoff_where_c}
        GROUP BY c.bldg_uid
        ORDER BY avg_rsrp DESC
    """)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- wireless_node_health per-cell aggregation --------------------
def wireless_node_health_cells(range_key: str) -> Dict[str, Any]:
    """Per-cell aggregation with cell location for map markers."""
    tbl = table("wireless_node_health")
    cells_tbl = table("wireless_cells")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE h.ts >= (SELECT MAX(ts) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    rows = run_query(f"""
        SELECT h.cell_id,
               ANY_VALUE(h.site_id)               AS site_id,
               ANY_VALUE(c.site_name)             AS site_name,
               ANY_VALUE(c.sector)                AS sector,
               ANY_VALUE(c.technology)            AS technology,
               ANY_VALUE(c.band)                  AS band,
               ANY_VALUE(c.latitude)              AS latitude,
               ANY_VALUE(c.longitude)             AS longitude,
               ANY_VALUE(c.vendor)                AS vendor,
               COUNT(*)                           AS record_count,
               SUM(CASE WHEN h.status='online'      THEN 1 ELSE 0 END) AS online_pings,
               SUM(CASE WHEN h.status='degraded'    THEN 1 ELSE 0 END) AS degraded_pings,
               SUM(CASE WHEN h.status='offline'     THEN 1 ELSE 0 END) AS offline_pings,
               SUM(CASE WHEN h.status='maintenance' THEN 1 ELSE 0 END) AS maintenance_pings,
               ROUND(AVG(h.prb_utilization_pct), 1)      AS avg_prb_pct,
               ROUND(AVG(h.active_users), 0)             AS avg_active_users,
               MAX(h.active_users)                        AS peak_active_users,
               ROUND(AVG(h.avg_dl_throughput_mbps), 1)   AS avg_dl_mbps,
               ROUND(AVG(h.avg_latency_ms), 1)           AS avg_latency_ms,
               SUM(CASE WHEN h.alarm_code IS NOT NULL THEN 1 ELSE 0 END) AS alarm_count,
               -- latest status (approximation: most frequent non-online status if any, else online)
               ANY_VALUE(CASE WHEN h.status='online' THEN 'online' END) AS any_online
        FROM {tbl} h
        LEFT JOIN {cells_tbl} c ON c.cell_id = h.cell_id
        {where_clause}
        GROUP BY h.cell_id
        ORDER BY record_count DESC
    """)
    # Derive a summary status per cell for coloring.
    for r in rows:
        tot = r["record_count"] or 1
        off_pct = 100.0 * (r["offline_pings"] or 0) / tot
        deg_pct = 100.0 * (r["degraded_pings"] or 0) / tot
        mnt_pct = 100.0 * (r["maintenance_pings"] or 0) / tot
        if off_pct >= 20:
            r["summary_status"] = "offline"
        elif deg_pct >= 20:
            r["summary_status"] = "degraded"
        elif mnt_pct >= 20:
            r["summary_status"] = "maintenance"
        else:
            r["summary_status"] = "online"
        r["uptime_pct"] = round(100.0 * (r["online_pings"] or 0) / tot, 1)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- mobility_presence per-building aggregation --------------------
def mobility_presence_buildings(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation for the mobility heatmap."""
    tbl = table("mobility_presence")
    atbl = table("sc_access_events")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    cutoff_cte = (
        f"cutoff AS (SELECT MAX(ts) - INTERVAL {_days} DAYS AS ts_min FROM {tbl}),"
        if _days else ""
    )
    cutoff_where = "WHERE ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    cutoff_where_m = "WHERE m.ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    cutoff_where_m2 = "WHERE m2.ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    rows = run_query(f"""
        WITH {cutoff_cte}
        geo AS (
          SELECT bldg_uid,
                 ANY_VALUE(latitude)  AS latitude,
                 ANY_VALUE(longitude) AS longitude
          FROM {atbl} GROUP BY bldg_uid
        ),
        cell_counts AS (
          SELECT bldg_uid, source_cell_id, COUNT(*) AS n
          FROM {tbl} m2
          {cutoff_where_m2}
          GROUP BY bldg_uid, source_cell_id
        ),
        dominant_cell AS (
          SELECT bldg_uid, source_cell_id,
                 ROW_NUMBER() OVER (PARTITION BY bldg_uid ORDER BY n DESC) AS rn
          FROM cell_counts
        )
        SELECT m.bldg_uid,
               ANY_VALUE(m.building_name)      AS building_name,
               ANY_VALUE(m.category)           AS category,
               ANY_VALUE(m.legacy_building_id) AS legacy_building_id,
               ANY_VALUE(g.latitude)           AS latitude,
               ANY_VALUE(g.longitude)          AS longitude,
               ANY_VALUE(dc.source_cell_id)    AS dominant_source_cell,
               COUNT(*)                                       AS total_records,
               COUNT(DISTINCT m.anon_device_id)               AS unique_devices,
               ROUND(AVG(m.dwell_seconds), 0)                 AS avg_dwell_seconds,
               MAX(m.device_count_in_building)                AS peak_concurrent,
               ROUND(AVG(m.signal_strength_dbm), 1)           AS avg_signal_dbm,
               SUM(CASE WHEN m.arrival_flag   THEN 1 ELSE 0 END) AS arrivals,
               SUM(CASE WHEN m.departure_flag THEN 1 ELSE 0 END) AS departures
        FROM {tbl} m
        LEFT JOIN geo g ON g.bldg_uid = m.bldg_uid
        LEFT JOIN dominant_cell dc ON dc.bldg_uid = m.bldg_uid AND dc.rn = 1
        {cutoff_where_m}
        GROUP BY m.bldg_uid
        ORDER BY unique_devices DESC
    """)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- wireless_network (combined view) --------------------
def wireless_network(range_key: str) -> Dict[str, Any]:
    """Combined view: connectivity buildings + cells with node_health status merged.

    This is a virtual dataset, surfaced only through the Datasets tab dropdown as
    "Telco Wireless Network". The map uses the dedicated
    /api/telemetry/wireless_network endpoint to fetch both building rows and
    merged cell rows in a single response.
    """
    # For the right-hand summary panel, reuse the connectivity summary.
    return wireless_connectivity(range_key)


def wireless_network_combined(range_key: str) -> Dict[str, Any]:
    """Returns {"buildings": [...], "cells": [...]} — merged dataset for map."""
    buildings_resp = wireless_connectivity_buildings(range_key)
    health_resp = wireless_node_health_cells(range_key)
    cells_tbl = table("wireless_cells")
    # Load cell metadata (one row per cell) for the merge.
    cell_meta_rows = run_query(f"""
        SELECT cell_id, site_id, site_name, sector, latitude, longitude,
               technology, band, frequency_mhz, bandwidth_mhz, azimuth_deg,
               pci, tac, vendor, plmn, height_m, status
        FROM {cells_tbl}
        ORDER BY site_id, sector, technology
    """)
    # Index node-health aggregates by cell_id for fast lookup.
    health_by_cell: Dict[str, Dict[str, Any]] = {
        (r.get("cell_id") or ""): r for r in (health_resp.get("rows") or [])
    }
    merged_cells: list = []
    for m in cell_meta_rows:
        cid = m.get("cell_id")
        h = health_by_cell.get(cid, {}) if cid else {}
        merged_cells.append({
            # From wireless_cells
            "cell_id":        m.get("cell_id"),
            "site_id":        m.get("site_id"),
            "site_name":      m.get("site_name"),
            "sector":         m.get("sector"),
            "latitude":       m.get("latitude"),
            "longitude":      m.get("longitude"),
            "technology":     m.get("technology"),
            "band":           m.get("band"),
            "frequency_mhz":  m.get("frequency_mhz"),
            "bandwidth_mhz":  m.get("bandwidth_mhz"),
            "azimuth_deg":    m.get("azimuth_deg"),
            "pci":            m.get("pci"),
            "tac":            m.get("tac"),
            "vendor":         m.get("vendor"),
            "plmn":           m.get("plmn"),
            "height_m":       m.get("height_m"),
            "static_status":  m.get("status"),
            # From wireless_node_health aggregate (may be None if no telemetry)
            "summary_status":  h.get("summary_status") or "online",
            "uptime_pct":      h.get("uptime_pct"),
            "avg_active_users": h.get("avg_active_users"),
            "peak_active_users": h.get("peak_active_users"),
            "alarm_count":     h.get("alarm_count") or 0,
            "online_pings":    h.get("online_pings") or 0,
            "offline_pings":   h.get("offline_pings") or 0,
            "degraded_pings":  h.get("degraded_pings") or 0,
            "maintenance_pings": h.get("maintenance_pings") or 0,
            "avg_prb_pct":     h.get("avg_prb_pct"),
            "avg_dl_mbps":     h.get("avg_dl_mbps"),
            "avg_latency_ms":  h.get("avg_latency_ms"),
            "record_count":    h.get("record_count") or 0,
        })
    return {
        "range": range_key,
        "buildings": buildings_resp.get("rows") or [],
        "building_count": buildings_resp.get("row_count") or 0,
        "cells": merged_cells,
        "cell_count": len(merged_cells),
    }


# -------------------- connectivity_presence --------------------
def connectivity_presence(range_key: str) -> Dict[str, Any]:
    """Per-building summary of Wi-Fi presence + modem motion derived from the
    telco-deployed broadband gateway fleet. The map heatmap is driven by the
    dedicated /api/telemetry/connectivity_presence/buildings endpoint."""
    tbl = table("connectivity_presence")
    where = _time_where("ts", tbl, range_key)
    agg = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name)        AS building_name,
               ANY_VALUE(category)             AS category,
               ANY_VALUE(legacy_building_id)   AS legacy_building_id,
               COUNT(*)                        AS record_count,
               COUNT(DISTINCT gateway_id)      AS gateway_count,
               AVG(connected_device_count)     AS avg_connected,
               MAX(connected_device_count)     AS peak_connected,
               AVG(unique_device_count_1h)     AS avg_unique_1h,
               SUM(CASE WHEN motion_detected THEN 1 ELSE 0 END) AS motion_events,
               AVG(wifi_signal_strength_dbm)   AS avg_wifi_dbm,
               AVG(bandwidth_used_mbps)        AS avg_bandwidth_mbps
        FROM {tbl}
        {where}
        GROUP BY bldg_uid
        ORDER BY avg_connected DESC
    """)
    preview = run_query(f"""
        SELECT ts, bldg_uid, building_name, category, gateway_id,
               connected_device_count, unique_device_count_1h, motion_detected,
               wifi_signal_strength_dbm
        FROM {tbl}
        {where}
        ORDER BY ts DESC
        LIMIT 10
    """)
    total_rec = sum(r["record_count"] for r in agg)
    return {
        "markers": [],
        "total_rows": total_rec,
        "unique_entities": len(agg),
        "aggregates": {
            "total_records": total_rec,
            "buildings_covered": len(agg),
            "total_gateways": sum(r["gateway_count"] or 0 for r in agg),
            "avg_connected_devices": round(
                sum((r["avg_connected"] or 0) * (r["record_count"] or 0) for r in agg) / max(total_rec, 1),
                1,
            ),
            "total_motion_events": sum(r["motion_events"] or 0 for r in agg),
        },
        "preview": preview,
    }


def connectivity_presence_buildings(range_key: str) -> Dict[str, Any]:
    """Per-building aggregation for the connectivity_presence heatmap."""
    tbl = table("connectivity_presence")
    gw_tbl = table("sc_gateway_network")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    cutoff_cte = (
        f"cutoff AS (SELECT MAX(ts) - INTERVAL {_days} DAYS AS ts_min FROM {tbl}),"
        if _days else ""
    )
    cutoff_where = "WHERE ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    cutoff_where_c = "WHERE c.ts >= (SELECT ts_min FROM cutoff)" if _days else ""
    rows = run_query(f"""
        WITH {cutoff_cte}
        gw AS (
          SELECT bldg_uid, COUNT(DISTINCT gateway_id) AS gateway_count
          FROM {gw_tbl}
          GROUP BY bldg_uid
        )
        SELECT c.bldg_uid,
               ANY_VALUE(c.building_name)       AS building_name,
               ANY_VALUE(c.category)            AS category,
               ANY_VALUE(c.legacy_building_id)  AS legacy_building_id,
               COUNT(*)                         AS record_count,
               ROUND(AVG(c.unique_device_count_1h), 1) AS unique_devices_per_hour,
               ROUND(AVG(c.connected_device_count), 1) AS avg_connected_devices,
               MAX(c.connected_device_count)    AS peak_connected_devices,
               ROUND(AVG(c.motion_events_1h), 1) AS motion_events_per_hour,
               SUM(CASE WHEN c.motion_detected THEN 1 ELSE 0 END) AS motion_events_total,
               ROUND(AVG(c.wifi_signal_strength_dbm), 1) AS avg_wifi_signal_dbm,
               ROUND(AVG(c.bandwidth_used_mbps), 1) AS avg_bandwidth_mbps,
               COALESCE(ANY_VALUE(gw.gateway_count), 0) AS gateway_count
        FROM {tbl} c
        LEFT JOIN gw ON gw.bldg_uid = c.bldg_uid
        {cutoff_where_c}
        GROUP BY c.bldg_uid
        ORDER BY unique_devices_per_hour DESC
    """)
    return {"range": range_key, "rows": rows, "row_count": len(rows)}


# -------------------- udl_campus_metrics --------------------
def udl_campus_metrics(range_key: str) -> Dict[str, Any]:
    """Right-hand summary panel for the UBC Urban Data Lake consolidated table."""
    tbl = table("udl_campus_metrics")
    # `date` column is DATE — use a narrow filter
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE date >= (SELECT MAX(date) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    agg = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name)        AS building_name,
               ANY_VALUE(category)             AS category,
               ANY_VALUE(legacy_building_id)   AS legacy_building_id,
               COUNT(*)                        AS day_count,
               AVG(occupancy_pct)              AS avg_occupancy_pct,
               AVG(energy_consumption_kwh)     AS avg_energy_kwh,
               AVG(avg_air_quality_index)      AS avg_aqi,
               AVG(pedestrian_count_daily)     AS avg_pedestrians,
               SUM(safety_incidents_count)     AS total_incidents,
               SUM(ev_sessions_count)          AS total_ev_sessions
        FROM {tbl}
        {where_clause}
        GROUP BY bldg_uid
        ORDER BY avg_occupancy_pct DESC
    """)
    preview = run_query(f"""
        SELECT date, bldg_uid, building_name, category, occupancy_pct,
               energy_consumption_kwh, safety_incidents_count, pedestrian_count_daily
        FROM {tbl}
        {where_clause}
        ORDER BY date DESC
        LIMIT 10
    """)
    total_records = sum(r["day_count"] for r in agg)
    return {
        "markers": [],
        "total_rows": total_records,
        "unique_entities": len(agg),
        "aggregates": {
            "total_building_days": total_records,
            "buildings_covered": len(agg),
            "campus_avg_occupancy_pct": round(
                sum((r["avg_occupancy_pct"] or 0) * (r["day_count"] or 0) for r in agg) / max(total_records, 1),
                1,
            ),
            "campus_total_incidents": sum(r["total_incidents"] or 0 for r in agg),
            "campus_total_ev_sessions": sum(r["total_ev_sessions"] or 0 for r in agg),
        },
        "preview": preview,
    }


# Allowed metric keys for the heatmap selector. Each maps to the underlying
# numeric column in udl_campus_metrics.
UDL_METRIC_COLS: Dict[str, str] = {
    "occupancy":   "occupancy_pct",
    "energy":      "energy_consumption_kwh",
    "air_quality": "avg_air_quality_index",
    "pedestrian":  "pedestrian_count_daily",
    "safety":      "safety_incidents_count",
}


def udl_campus_metrics_buildings(range_key: str, metric: str = "occupancy") -> Dict[str, Any]:
    """Per-building aggregation for the UDL heatmap. Returns ALL UDL signals
    averaged over the window plus an `active_metric_value` column matching
    the requested metric so the frontend can colour the heatmap."""
    metric_col = UDL_METRIC_COLS.get(metric, "occupancy_pct")
    tbl = table("udl_campus_metrics")
    _days_map = {"24h": 1, "7d": 7, "30d": 30}
    _days = _days_map.get(range_key)
    where_clause = (
        f"WHERE date >= (SELECT MAX(date) - INTERVAL {_days} DAYS FROM {tbl})"
        if _days else ""
    )
    # For "safety" we want SUM (count of incidents over window), not AVG.
    if metric == "safety":
        active_metric_expr = "SUM(safety_incidents_count)"
    elif metric == "pedestrian":
        active_metric_expr = "ROUND(AVG(pedestrian_count_daily), 0)"
    else:
        active_metric_expr = f"ROUND(AVG({metric_col}), 2)"

    rows = run_query(f"""
        SELECT bldg_uid,
               ANY_VALUE(building_name)        AS building_name,
               ANY_VALUE(category)             AS category,
               ANY_VALUE(legacy_building_id)   AS legacy_building_id,
               COUNT(*)                        AS day_count,
               ROUND(AVG(occupancy_pct), 1)              AS avg_occupancy_pct,
               ROUND(AVG(peak_occupancy_count), 0)       AS avg_peak_occupancy,
               MAX(peak_occupancy_count)                 AS max_peak_occupancy,
               ROUND(AVG(hvac_load_kwh), 1)              AS avg_hvac_load_kwh,
               ROUND(AVG(energy_consumption_kwh), 1)     AS avg_energy_kwh,
               ROUND(AVG(water_consumption_m3), 2)       AS avg_water_m3,
               SUM(ev_sessions_count)                    AS total_ev_sessions,
               ROUND(SUM(ev_energy_delivered_kwh), 1)    AS total_ev_kwh,
               ROUND(AVG(avg_air_quality_index), 1)      AS avg_aqi,
               ROUND(AVG(avg_pm25), 2)                   AS avg_pm25,
               ROUND(AVG(avg_temperature_c), 1)          AS avg_temperature_c,
               ROUND(AVG(avg_humidity_pct), 1)           AS avg_humidity_pct,
               ROUND(AVG(pedestrian_count_daily), 0)     AS avg_pedestrians,
               ROUND(AVG(vehicle_count_daily), 0)        AS avg_vehicles,
               SUM(safety_incidents_count)               AS total_incidents,
               -- Most-frequent comma-joined union of incident type strings
               ARRAY_JOIN(
                 ARRAY_DISTINCT(
                   FILTER(
                     SPLIT(
                       CONCAT_WS(',', COLLECT_LIST(safety_incident_types)),
                       ','
                     ),
                     x -> x IS NOT NULL AND x <> ''
                   )
                 ),
                 ', '
               ) AS safety_types,
               {active_metric_expr} AS active_metric_value
        FROM {tbl}
        {where_clause}
        GROUP BY bldg_uid
        ORDER BY active_metric_value DESC
    """)
    return {
        "range": range_key,
        "metric": metric,
        "metric_column": metric_col,
        "rows": rows,
        "row_count": len(rows),
    }


DATASETS: Dict[str, Tuple[str, Any, bool]] = {
    # key: (label, handler, supports_time_filter)
    "dim_buildings":         ("UBC Residences (dim_buildings)",        dim_buildings,         False),
    "dim_zones":             ("Campus Zones (dim_zones)",              dim_zones,             False),
    "sc_access_events":     ("Building Access (sc_access_events)",   sc_access_events,     True),
    "sc_device_health":     ("Device Telemetry (sc_device_health)",  sc_device_health,     True),
    "mobility_presence":     ("Telco Mobility (mobility_presence)",   mobility_presence,     True),
    "wireless_cells":        ("Telco Wireless Cells (wireless_cells)", wireless_cells,       False),
    "wireless_connectivity": ("Telco Wireless Coverage (wireless_connectivity)", wireless_connectivity, True),
    "wireless_node_health":  ("Telco Cell KPIs (wireless_node_health)", wireless_node_health, True),
    "wireless_network":      ("Telco Wireless Network (wireless_network)", wireless_network, True),
    "gateway_telemetry":     ("Broadband Gateways (gateway_telemetry)",  gateway_telemetry,     True),
    "udl_ev_charging":       ("EV Charging (udl_ev_charging)",         udl_ev_charging,       True),
    "udl_environmental":     ("Environmental Sensors (udl_environmental)", udl_environmental, True),
    "connectivity_presence": ("Connectivity Presence (connectivity_presence)",
                              connectivity_presence,                                            True),
    "udl_campus_metrics":    ("UBC Urban Data Lake (udl_campus_metrics)",
                              udl_campus_metrics,                                               True),
}
