from __future__ import annotations

import httpx
import structlog

from llogr.auth import AuthContext
from llogr.config import Settings
from llogr.metrics import CLICKBEAT_FORWARD_ERRORS, CLICKBEAT_FORWARD_SECONDS
from llogr.models import IngestionEvent

from datetime import datetime

logger = structlog.get_logger(__name__)


def transform_events(events: list[IngestionEvent], auth: AuthContext) -> list[dict]:
    return [
        {
            "event_id": event.id,
            "event_type": event.type,
            "timestamp": event.timestamp,
            "project_id": auth.public_key,
            "payload": event.body,
        }
        for event in events
    ]


async def send_to_clickbeat(
    events: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
) -> None:
    cb_cfg = settings.clickbeat
    transformed = transform_events(events, auth)

    with CLICKBEAT_FORWARD_SECONDS.time():
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    cb_cfg.api_url,
                    json=transformed,
                    headers={
                        "Authorization": f"Bearer {cb_cfg.api_key}",
                        "Content-Type": "application/json",
                    },
                )
                resp.raise_for_status()
        except Exception:
            CLICKBEAT_FORWARD_ERRORS.inc()
            raise

    logger.info("forwarded_to_clickbeat", events=len(transformed))


async def search_via_clickbeat(
    query: str,
    project_id: str,
    settings: Settings,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Query events through ClickBeat's query API."""
    cb_cfg = settings.clickbeat
    if not cb_cfg.query_url:
        logger.warning("clickbeat_query_url_not_configured")
        return []

    params: dict = {
        "q": query,
        "project_id": project_id,
        "limit": min(limit, 500),
    }
    if start:
        params["start"] = start.isoformat()
    if end:
        params["end"] = end.isoformat()
    if session_id:
        params["session_id"] = session_id
    if trace_id:
        params["trace_id"] = trace_id

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                cb_cfg.query_url,
                params=params,
                headers={
                    "Authorization": f"Bearer {cb_cfg.api_key}",
                    "Accept": "application/json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

        results = []
        for row in data if isinstance(data, list) else data.get("results", data.get("data", [])):
            results.append({
                "id": row.get("event_id", row.get("id", "")),
                "type": row.get("event_type", row.get("type", "")),
                "timestamp": row.get("timestamp", ""),
                "body": row.get("payload", row.get("body", {})),
            })
        return results
    except Exception as e:
        logger.error("clickbeat_query_failed", error=str(e))
        return []
