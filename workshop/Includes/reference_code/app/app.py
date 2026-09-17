"""Smart Community Testbed - FastAPI backend + static React frontend."""
from __future__ import annotations
import os
import datetime
import decimal
import json
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from server.datasets import (
    DATASETS,
    device_health_buildings,
    access_events_buildings,
    wireless_connectivity_buildings,
    wireless_node_health_cells,
    wireless_network_combined,
    mobility_presence_buildings,
    connectivity_presence_buildings,
    udl_campus_metrics_buildings,
)
from server.genie import (
    ask as genie_ask,
    space_info as genie_space_info,
    GenieSpaceNotConfigured,
)
from server.geometry import get_building_polygons, get_zone_polygons, get_campus_polygons

app = FastAPI(title="Smart Community Testbed")


def _json_default(o: Any):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    if isinstance(o, bytes):
        return o.decode("utf-8", errors="replace")
    raise TypeError(f"Unserializable: {type(o).__name__}")


def _json(payload: Any) -> JSONResponse:
    # FastAPI's default JSON encoder doesn't handle datetimes/decimals from DBSQL.
    return JSONResponse(content=json.loads(json.dumps(payload, default=_json_default)))


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/datasets")
def list_datasets():
    return [
        {"key": key, "label": label, "supports_time_filter": tf}
        for key, (label, _, tf) in DATASETS.items()
    ]


@app.get("/api/data/{dataset_key}")
def get_data(dataset_key: str, range: str = Query("all", pattern="^(24h|7d|30d|all)$")):
    if dataset_key not in DATASETS:
        raise HTTPException(404, detail=f"Unknown dataset: {dataset_key}")
    _, handler, _ = DATASETS[dataset_key]
    try:
        result = handler(range)
    except Exception as e:
        raise HTTPException(500, detail=f"Query failed: {type(e).__name__}: {e}")
    return _json(result)


# -------- Real UBC GeoJSON footprints --------
@app.get("/api/geojson/buildings")
def geojson_buildings():
    try:
        return _json(get_building_polygons())
    except Exception as e:
        raise HTTPException(500, detail=f"Buildings geojson failed: {type(e).__name__}: {e}")


@app.get("/api/geojson/zones")
def geojson_zones():
    try:
        return _json(get_zone_polygons())
    except Exception as e:
        raise HTTPException(500, detail=f"Zones geojson failed: {type(e).__name__}: {e}")


@app.get("/api/geojson/campus")
def geojson_campus():
    try:
        return _json(get_campus_polygons())
    except Exception as e:
        raise HTTPException(500, detail=f"Campus geojson failed: {type(e).__name__}: {e}")


# -------- Per-building telemetry aggregations (for heatmaps) --------
@app.get("/api/telemetry/device_health/buildings")
def telemetry_device_health_buildings(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(device_health_buildings(window))
    except Exception as e:
        raise HTTPException(500, detail=f"device_health buildings failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/access_events/buildings")
def telemetry_access_events_buildings(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(access_events_buildings(window))
    except Exception as e:
        raise HTTPException(500, detail=f"access_events buildings failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/wireless_connectivity/buildings")
def telemetry_wireless_connectivity_buildings(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(wireless_connectivity_buildings(window))
    except Exception as e:
        raise HTTPException(500, detail=f"wireless_connectivity buildings failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/wireless_node_health/cells")
def telemetry_wireless_node_health_cells(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(wireless_node_health_cells(window))
    except Exception as e:
        raise HTTPException(500, detail=f"wireless_node_health cells failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/wireless_network")
def telemetry_wireless_network(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    """Combined: building-level connectivity (RSRP heatmap) + per-cell metadata
    merged with node_health status (green/amber/red/gray + active users + alarms)."""
    try:
        return _json(wireless_network_combined(window))
    except Exception as e:
        raise HTTPException(500, detail=f"wireless_network failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/mobility_presence/buildings")
def telemetry_mobility_presence_buildings(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(mobility_presence_buildings(window))
    except Exception as e:
        raise HTTPException(500, detail=f"mobility_presence buildings failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/connectivity_presence/buildings")
def telemetry_connectivity_presence_buildings(window: str = Query("30d", pattern="^(24h|7d|30d|all)$")):
    try:
        return _json(connectivity_presence_buildings(window))
    except Exception as e:
        raise HTTPException(500, detail=f"connectivity_presence buildings failed: {type(e).__name__}: {e}")


@app.get("/api/telemetry/udl_campus_metrics/buildings")
def telemetry_udl_campus_metrics_buildings(
    window: str = Query("30d", pattern="^(24h|7d|30d|all)$"),
    metric: str = Query("occupancy", pattern="^(occupancy|energy|air_quality|pedestrian|safety)$"),
):
    try:
        return _json(udl_campus_metrics_buildings(window, metric))
    except Exception as e:
        raise HTTPException(500, detail=f"udl_campus_metrics buildings failed: {type(e).__name__}: {e}")


# -------- Genie Q&A --------
class GenieAsk(BaseModel):
    question: str
    conversation_id: Optional[str] = None


@app.get("/api/genie/info")
def genie_info():
    return genie_space_info()


@app.post("/api/genie/ask")
def genie_ask_endpoint(body: GenieAsk):
    try:
        return _json(genie_ask(body.question, conversation_id=body.conversation_id))
    except GenieSpaceNotConfigured as e:
        # Return a clear user-facing payload so the UI can show a friendly message.
        return _json({
            "status": "FAILED",
            "error": "Genie space not configured — contact admin.",
            "text": None, "sql": None, "columns": None, "rows": None,
            "conversation_id": None, "message_id": None,
            "detail": str(e),
        })
    except Exception as e:
        raise HTTPException(500, detail=f"Genie request failed: {type(e).__name__}: {e}")


# -------- Floating chat (Genie-backed) --------
class ChatMessage(BaseModel):
    message: str
    conversation_id: Optional[str] = None


@app.post("/api/chat")
def chat(body: ChatMessage):
    try:
        return _json(genie_ask(body.message, conversation_id=body.conversation_id))
    except GenieSpaceNotConfigured as e:
        return _json({
            "status": "FAILED",
            "error": "Genie space not configured — contact admin.",
            "text": None, "sql": None, "columns": None, "rows": None,
            "conversation_id": None, "message_id": None,
            "detail": str(e),
        })
    except Exception as e:
        raise HTTPException(500, detail=f"Chat failed: {type(e).__name__}: {e}")


# -------- Static React frontend --------
FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"

if FRONTEND_DIST.exists():
    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/")
    def root():
        return FileResponse(FRONTEND_DIST / "index.html")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(404)
        candidate = FRONTEND_DIST / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(FRONTEND_DIST / "index.html")
else:
    @app.get("/")
    def root():
        return {
            "message": "Frontend not built yet. Run 'npm run build' in frontend/.",
            "api": "/api/datasets",
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
