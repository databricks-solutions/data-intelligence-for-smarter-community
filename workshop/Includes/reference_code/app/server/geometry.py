"""UBC real-world building and zone footprints from UBCGeodata (CC BY 4.0).

Loads GeoJSON files once on startup and merges in the dim_buildings/dim_zones
rows from Delta tables so the map renders real UBC polygons with up-to-date
table properties.

Source: UBCGeodata (Abacus Library), UBC Campus & Community Planning.
https://abacus.library.ubc.ca/dataset.xhtml?persistentId=hdl:11272.1/AB2/S15BIR

This module resolves 38 residences via BCOM_UID (bcom_uid) in
``ubcv_complexes.geojson`` with a NAME-substring fallback for the two legacy
slots whose geometry still lives in ``ubcv_buildings.geojson`` (Orchard
Commons and Exchange Residence).  The legacy slots are only consulted if the
Delta row's ``bcom_uid`` is NULL.
"""
from __future__ import annotations
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import table
from .sql import run_query


GEOJSON_DIR = Path(__file__).parent / "geojson"

# Cached GeoJSON at module load time.
_complexes: Optional[Dict[str, Any]] = None
_buildings: Optional[Dict[str, Any]] = None
_neighbourhoods: Optional[Dict[str, Any]] = None

# Pre-built BCOM_UID index (populated lazily on first call).
_complexes_by_uid: Optional[Dict[str, Dict[str, Any]]] = None


def _load_geojson(name: str) -> Dict[str, Any]:
    path = GEOJSON_DIR / name
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_loaded() -> None:
    global _complexes, _buildings, _neighbourhoods, _complexes_by_uid
    if _complexes is None:
        _complexes = _load_geojson("ubcv_complexes.geojson")
    if _buildings is None:
        _buildings = _load_geojson("ubcv_buildings.geojson")
    if _neighbourhoods is None:
        _neighbourhoods = _load_geojson("ubcv_neighbourhoods.geojson")
    if _complexes_by_uid is None:
        idx: Dict[str, Dict[str, Any]] = {}
        for feat in _complexes.get("features", []):  # type: ignore[union-attr]
            props = feat.get("properties") or {}
            uid = props.get("BCOM_UID")
            if uid:
                idx[str(uid)] = feat
        _complexes_by_uid = idx


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _find_feature(
    fc: Dict[str, Any], name_match: str, contains: bool = False
) -> Optional[Dict[str, Any]]:
    """Find first feature whose NAME matches exactly (or contains if contains=True)."""
    target = _norm(name_match)
    for feat in fc.get("features", []):
        props = feat.get("properties", {}) or {}
        name = _norm(props.get("NAME"))
        if contains:
            if target in name or name in target:
                return feat
        else:
            if name == target:
                return feat
    return None


def _find_features_contains(
    fc: Dict[str, Any], needles: List[str]
) -> List[Dict[str, Any]]:
    """Find all features whose NAME contains any of the given needles."""
    out: List[Dict[str, Any]] = []
    norms = [_norm(n) for n in needles]
    for feat in fc.get("features", []):
        props = feat.get("properties", {}) or {}
        name = _norm(props.get("NAME"))
        if any(n in name for n in norms):
            out.append(feat)
    return out


# Legacy fallback mapping for building_ids whose dim row has no bcom_uid.
# This covers the two flagship slots (BLDG_004 Orchard Commons, BLDG_008
# Exchange Residence) kept in the pre-38-building dim schema.  With the new
# 38-building dim these slots hold different bcom_uids, so the lookup-by-uid
# path normally wins; this map is only consulted when bcom_uid is NULL.
LEGACY_BUILDING_MAP: Dict[str, Dict[str, Any]] = {
    # Orchard Commons - merge Vantage College + Childcare footprints
    "BLDG_004": {
        "source": "buildings_multi",
        "names": [
            "Orchard Commons - Vantage College",
            "Orchard Commons - Childcare",
        ],
    },
    "BLDG_008": {"source": "buildings", "names": ["Exchange Residence"]},
}


# Mapping: our zone_name -> (source file, NAME, contains?). None means fall back to circle.
ZONE_MAP: Dict[str, Optional[Dict[str, Any]]] = {
    "AMS Student Nest": {"source": "buildings", "name": "AMS Student Nest"},
    "Irving K. Barber Learning Centre": {
        "source": "buildings",
        "name": "Irving K. Barber Learning Centre",
    },
    "Koerner Library": {"source": "buildings", "name": "Walter C. Koerner Library"},
    "UBC Aquatic Centre": {"source": "buildings", "name": "UBC Aquatic Centre"},
    "UBC Bookstore": {"source": "buildings", "name": "UBC Bookstore / NCE"},
    "War Memorial Gym": {"source": "buildings", "name": "War Memorial Gymnasium"},
    "Life Sciences Institute": {"source": "buildings", "name": "Life Sciences Centre"},
    "Nitobe Memorial Garden": {"source": "buildings", "name": "Nitobe Memorial Garden"},
    "Rose Garden": {"source": "buildings", "name": "Rose Garden Parkade"},
    "Wesbrook Village": {
        "source": "neighbourhoods",
        "name": "Wesbrook Place",
        "contains": True,
    },
    # Fall back to small circles
    "Thunderbird Arena": None,
    "UBC Bus Loop": None,
    "MacInnes Field": None,
    "UBC Farm": None,
    "Main Mall Central": None,
    "Student Union Blvd Transit": None,
}


def circle_polygon(
    lat: float, lon: float, radius_m: float = 60, n: int = 24
) -> Dict[str, Any]:
    """Return a GeoJSON Polygon approximating a circle centered at (lat, lon)."""
    # Convert meters to degrees. At UBC latitude (~49.26°N):
    # 1° lat ≈ 111,320 m   1° lon ≈ cos(lat) * 111,320 m
    deg_lat = radius_m / 111_320.0
    cos_lat = math.cos(math.radians(lat))
    deg_lon = radius_m / (111_320.0 * max(cos_lat, 1e-6))
    coords: List[List[float]] = []
    for i in range(n):
        theta = 2 * math.pi * (i / n)
        dx = math.cos(theta) * deg_lon
        dy = math.sin(theta) * deg_lat
        coords.append([lon + dx, lat + dy])
    coords.append(coords[0])  # close ring
    return {"type": "Polygon", "coordinates": [coords]}


def _merge_multipolygons(features: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combine multiple features into one MultiPolygon geometry."""
    rings: List[Any] = []
    for f in features:
        geom = f.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates") or []
        if gtype == "Polygon":
            rings.append(coords)
        elif gtype == "MultiPolygon":
            rings.extend(coords)
    if len(rings) == 1:
        return {"type": "Polygon", "coordinates": rings[0]}
    return {"type": "MultiPolygon", "coordinates": rings}


def _resolve_legacy_building_geom(mapping: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return GeoJSON geometry for a legacy (pre-38) building mapping."""
    _ensure_loaded()
    source = mapping.get("source")
    names = mapping.get("names", [])
    if source == "complexes":
        for nm in names:
            feat = _find_feature(_complexes, nm, contains=True)
            if feat and feat.get("geometry"):
                return feat["geometry"]
    elif source == "buildings":
        for nm in names:
            feat = _find_feature(_buildings, nm, contains=False)
            if feat and feat.get("geometry"):
                return feat["geometry"]
    elif source == "buildings_multi":
        feats = _find_features_contains(_buildings, names)
        if feats:
            return _merge_multipolygons(feats)
    return None


def _resolve_building_geom_by_uid(bcom_uid: Optional[str]) -> Optional[Dict[str, Any]]:
    """Lookup a complex polygon from ubcv_complexes.geojson by BCOM_UID."""
    if not bcom_uid:
        return None
    _ensure_loaded()
    feat = (_complexes_by_uid or {}).get(str(bcom_uid))
    if feat and feat.get("geometry"):
        return feat["geometry"]
    return None


def _resolve_building_geom_by_name(
    building_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Fallback: look up a polygon by NAME in ubcv_complexes.geojson."""
    if not building_name:
        return None
    _ensure_loaded()
    # Try contains on complexes first
    feat = _find_feature(_complexes, building_name, contains=True)
    if feat and feat.get("geometry"):
        return feat["geometry"]
    # Then buildings
    feat = _find_feature(_buildings, building_name, contains=True)
    if feat and feat.get("geometry"):
        return feat["geometry"]
    return None


def _resolve_zone_geom(
    mapping: Optional[Dict[str, Any]], lat: float, lon: float
) -> Tuple[Dict[str, Any], bool]:
    """Return (geometry, matched_real_polygon)."""
    _ensure_loaded()
    if mapping is None:
        return circle_polygon(lat, lon, radius_m=60), False
    source = mapping.get("source")
    name = mapping.get("name", "")
    contains = bool(mapping.get("contains", False))
    fc = None
    if source == "buildings":
        fc = _buildings
    elif source == "complexes":
        fc = _complexes
    elif source == "neighbourhoods":
        fc = _neighbourhoods
    if fc is not None:
        feat = _find_feature(fc, name, contains=contains)
        if feat and feat.get("geometry"):
            return feat["geometry"], True
    return circle_polygon(lat, lon, radius_m=60), False


def get_building_polygons() -> Dict[str, Any]:
    """Return a GeoJSON FeatureCollection for dim_buildings using real UBC geometry.

    Resolution order for each row:
      1) Match by ``bcom_uid`` against ubcv_complexes.geojson (preferred).
      2) Match by building_name substring (fallback).
      3) Legacy hard-coded BLDG_* mapping for pre-38 slots (Orchard, Exchange).
      4) Small circle at stored lat/lon (last resort).
    """
    rows = run_query(
        f"""
        SELECT building_id, bcom_uid, building_name, address, latitude, longitude,
               n_units, complex_type, year_built
        FROM {table('dim_buildings')}
        ORDER BY building_id
        """
    )
    features: List[Dict[str, Any]] = []
    unmatched: List[str] = []
    for row in rows:
        bid = row.get("building_id")
        uid = row.get("bcom_uid")
        bname = row.get("building_name")
        geom: Optional[Dict[str, Any]] = None
        if uid:
            geom = _resolve_building_geom_by_uid(str(uid))
        if geom is None and bname:
            geom = _resolve_building_geom_by_name(str(bname))
        if geom is None:
            legacy = LEGACY_BUILDING_MAP.get(bid)
            if legacy:
                geom = _resolve_legacy_building_geom(legacy)
        if geom is None:
            unmatched.append(f"{bid}:{bname}")
            geom = circle_polygon(
                float(row["latitude"]), float(row["longitude"]), radius_m=50
            )
        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "building_id": row["building_id"],
                    "bcom_uid": row.get("bcom_uid"),
                    "building_name": row["building_name"],
                    "address": row.get("address"),
                    "n_units": row["n_units"],
                    "complex_type": row["complex_type"],
                    "year_built": row["year_built"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {"unmatched": unmatched},
    }


# -------------------- Campus Map (all 444 UBC buildings) --------------------

# Map UBCGeodata BLDG_USAGE values to user-facing categories.
USAGE_TO_CATEGORY: Dict[str, str] = {
    "Housing": "Residential",
    "StudentHousing": "Residential",
    "Academic": "Academic",
    "Services": "Services",
    "Research": "Research",
    "Operations": "Operations",
    "Administrative": "Administrative",
    "Athletics": "Athletics",
    "Commons": "Commons",
    "Parking": "Parking",
    "Commercial": "Commercial",
    "Other": "Other",
}


def _normalize_usage(usage: Optional[str]) -> str:
    if usage is None:
        return "Other"
    s = str(usage).strip()
    return s if s else "Other"


def _categorize(usage: Optional[str]) -> str:
    return USAGE_TO_CATEGORY.get(_normalize_usage(usage), "Other")


def get_campus_polygons() -> Dict[str, Any]:
    """Return a GeoJSON FeatureCollection of ALL UBC Vancouver buildings
    (444 features), annotated with a user-facing ``category`` derived from
    the UBCGeodata ``BLDG_USAGE`` attribute.  Includes a ``meta`` block with
    per-category counts and up-to-5 example names.
    """
    _ensure_loaded()
    src_features = (_buildings or {}).get("features", [])  # type: ignore[union-attr]
    features: List[Dict[str, Any]] = []
    category_counts: Dict[str, int] = {}
    category_samples: Dict[str, List[str]] = {}
    for feat in src_features:
        geom = feat.get("geometry")
        if not geom:
            continue
        props = feat.get("properties") or {}
        usage = _normalize_usage(props.get("BLDG_USAGE"))
        category = _categorize(props.get("BLDG_USAGE"))
        name = props.get("NAME") or props.get("SHORTNAME") or props.get("BLDG_CODE") or ""
        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "name": name,
                    "bldg_uid": props.get("BLDG_UID"),
                    "bldg_code": props.get("BLDG_CODE"),
                    "address": props.get("PRIMARY_ADDRESS"),
                    "usage": usage,
                    "category": category,
                    "neighbourhood": props.get("NEIGHBOURHOOD"),
                    "max_floors": props.get("MAX_FLOORS"),
                    "bldg_height": props.get("BLDG_HEIGHT"),
                },
            }
        )
        category_counts[category] = category_counts.get(category, 0) + 1
        samples = category_samples.setdefault(category, [])
        if name and len(samples) < 5:
            samples.append(name)

    # Ensure the canonical 11 categories are all represented in meta even
    # when a category is empty (so the frontend summary table is stable).
    canonical = [
        "Residential",
        "Academic",
        "Services",
        "Research",
        "Operations",
        "Administrative",
        "Athletics",
        "Commons",
        "Parking",
        "Commercial",
        "Other",
    ]
    category_order: List[Dict[str, Any]] = []
    for cat in canonical:
        category_order.append(
            {
                "category": cat,
                "count": category_counts.get(cat, 0),
                "examples": category_samples.get(cat, []),
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "total_buildings": len(features),
            "category_counts": category_counts,
            "category_samples": category_samples,
            "categories": category_order,
        },
    }


def get_zone_polygons() -> Dict[str, Any]:
    """Return a GeoJSON FeatureCollection for dim_zones using real UBC geometry."""
    rows = run_query(
        f"""
        SELECT zone_id, zone_name, zone_type, latitude, longitude
        FROM {table('dim_zones')}
        ORDER BY zone_id
        """
    )
    features: List[Dict[str, Any]] = []
    fallback_zones: List[str] = []
    for row in rows:
        zname = row.get("zone_name") or ""
        mapping = ZONE_MAP.get(zname, None)
        lat = float(row["latitude"]) if row.get("latitude") is not None else 49.2606
        lon = float(row["longitude"]) if row.get("longitude") is not None else -123.2460
        geom, matched = _resolve_zone_geom(mapping, lat, lon)
        if not matched:
            fallback_zones.append(zname)
        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "zone_id": row["zone_id"],
                    "zone_name": row["zone_name"],
                    "zone_type": row["zone_type"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "geometry_matched": matched,
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {"fallback_zones": fallback_zones},
    }
