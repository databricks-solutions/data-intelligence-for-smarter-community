"""Databricks Genie Conversations API client.

Wraps:
  POST /api/2.0/genie/spaces/{space_id}/start-conversation
  POST /api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages       (create)
  GET  /api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}
  GET  /api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}/attachments/{aid}/query-result

Returns a normalized payload:
  {
    "conversation_id": str,
    "message_id": str,
    "status": "COMPLETED" | "FAILED" | ...,
    "text": str | None,                       # assistant text answer
    "sql": str | None,                        # generated SQL (if any)
    "columns": [str] | None,
    "rows": [[val,...]] | None,
    "error": str | None,
  }
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests

from .config import get_oauth_token, get_workspace_host

# Lab 06 writes this into app.yaml at deploy time from the Genie space you create in
# Lab 05. Empty by default: the app must be told which space to use — never a hardcoded one.
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}


class GenieSpaceNotConfigured(Exception):
    """Raised when the configured Genie space is missing or inaccessible to the app."""


class GenieAPIError(Exception):
    """Raised for non-404 Genie API errors."""


def _auth_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {get_oauth_token()}",
        "Content-Type": "application/json",
    }


def _api(path: str, method: str = "GET", body: Optional[Dict] = None) -> Dict[str, Any]:
    url = f"{get_workspace_host().rstrip('/')}{path}"
    resp = requests.request(method, url, headers=_auth_headers(), json=body, timeout=60)
    if resp.status_code == 404:
        raise GenieSpaceNotConfigured(
            "Genie space is not configured or the app lacks permission to access it. "
            "Contact an admin to grant CAN_RUN on the Genie space to the app service principal."
        )
    if resp.status_code == 403:
        raise GenieSpaceNotConfigured(
            "App service principal does not have permission to run the Genie space. "
            "Contact an admin to grant CAN_RUN permission."
        )
    if resp.status_code >= 400:
        raise GenieAPIError(f"Genie API {method} {path} -> {resp.status_code}: {resp.text[:500]}")
    return resp.json()


def _wait_for_message(space_id: str, cid: str, mid: str, poll_s: float = 1.5, timeout_s: float = 90) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    path = f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}"
    last = None
    while time.time() < deadline:
        last = _api(path)
        state = last.get("status") or last.get("state") or ""
        if state in _TERMINAL_STATES:
            return last
        time.sleep(poll_s)
    return last or {}


def _extract_query_result(space_id: str, cid: str, mid: str, attachment_id: str) -> Dict[str, Any]:
    path = f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}/attachments/{attachment_id}/query-result"
    return _api(path)


def _parse_message(space_id: str, msg: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "conversation_id": msg.get("conversation_id"),
        "message_id": msg.get("id") or msg.get("message_id"),
        "status": msg.get("status") or msg.get("state"),
        "text": None,
        "sql": None,
        "columns": None,
        "rows": None,
        "error": None,
    }
    if out["status"] in {"FAILED", "CANCELLED"}:
        out["error"] = msg.get("error") or msg.get("message", "Genie returned no answer.")

    # content (text response): in new API it's `content` (user message) — but the assistant
    # response is inside `attachments`.
    attachments = msg.get("attachments") or []
    for att in attachments:
        if "text" in att and isinstance(att["text"], dict):
            out["text"] = att["text"].get("content") or out["text"]
        elif att.get("content"):
            out["text"] = att["content"]
        if "query" in att and isinstance(att["query"], dict):
            out["sql"] = att["query"].get("query") or att["query"].get("query_text")
            att_id = att.get("attachment_id") or att.get("id")
            if att_id:
                try:
                    qr = _extract_query_result(space_id, out["conversation_id"], out["message_id"], att_id)
                    sr = qr.get("statement_response") or qr
                    manifest = sr.get("manifest") or {}
                    schema = manifest.get("schema") or {}
                    cols = [c.get("name") for c in (schema.get("columns") or [])]
                    result = sr.get("result") or {}
                    data = result.get("data_array") or []
                    out["columns"] = cols
                    out["rows"] = data
                except Exception as e:  # noqa: BLE001
                    out["error"] = f"query-result fetch failed: {e}"

    # fallback: some message payloads put the answer in 'content'
    if out["text"] is None and isinstance(msg.get("content"), str) and out["status"] == "COMPLETED":
        out["text"] = msg["content"]
    return out


def ask(question: str, conversation_id: Optional[str] = None, space_id: Optional[str] = None) -> Dict[str, Any]:
    """Submit a question to Genie. If conversation_id is provided, continue it."""
    sid = space_id or GENIE_SPACE_ID
    if not sid:
        raise GenieSpaceNotConfigured(
            "No Genie space is configured. Lab 06 sets GENIE_SPACE_ID in app.yaml from "
            "the space you create in Lab 05 — re-run Lab 06 once your Space ID is saved."
        )
    if not conversation_id:
        start = _api(
            f"/api/2.0/genie/spaces/{sid}/start-conversation",
            method="POST",
            body={"content": question},
        )
        cid = start.get("conversation_id") or (start.get("conversation") or {}).get("id")
        mid = start.get("message_id") or (start.get("message") or {}).get("id")
    else:
        cid = conversation_id
        msg = _api(
            f"/api/2.0/genie/spaces/{sid}/conversations/{cid}/messages",
            method="POST",
            body={"content": question},
        )
        mid = msg.get("id") or msg.get("message_id")

    if not cid or not mid:
        return {"error": "Genie did not return conversation/message ids",
                "conversation_id": cid, "message_id": mid,
                "status": "FAILED", "text": None, "sql": None, "columns": None, "rows": None}

    final = _wait_for_message(sid, cid, mid)
    parsed = _parse_message(sid, final)
    parsed["space_id"] = sid
    return parsed


def space_info() -> Dict[str, Any]:
    """Return basic metadata about the configured Genie space (used by frontend)."""
    if not GENIE_SPACE_ID:
        return {"space_id": "", "configured": False, "title": None,
                "description": None, "workspace_url": None}
    try:
        data = _api(f"/api/2.0/data-rooms/{GENIE_SPACE_ID}")
    except Exception:  # noqa: BLE001
        data = {}
    host = get_workspace_host().rstrip("/")
    return {
        "space_id": GENIE_SPACE_ID,
        "configured": True,
        "title": data.get("display_name") or "Ask Genie",
        "description": data.get("description"),
        "workspace_url": f"{host}/genie/rooms/{GENIE_SPACE_ID}",
    }
