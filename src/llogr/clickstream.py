"""Clickstream client — sends events in Amplitude HTTP V2 API format (ingest only)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

import httpx
import structlog

from llogr.auth import AuthContext
from llogr.config import Settings
from llogr.metrics import CLICKSTREAM_FORWARD_ERRORS, CLICKSTREAM_FORWARD_SECONDS
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)


def _iso_to_epoch_ms(ts: str) -> int:
    """Convert ISO timestamp to epoch milliseconds."""
    try:
        return int(datetime.fromisoformat(ts).timestamp() * 1000)
    except (ValueError, TypeError):
        return int(datetime.now().timestamp() * 1000)


def transform_to_amplitude(events: list[IngestionEvent], auth: AuthContext, input_hash: str = "") -> list[dict]:
    """Transform llogr events into Amplitude event format."""
    amplitude_events = []
    for event in events:
        amplitude_events.append({
            "user_id": auth.public_key,
            "device_id": f"llogr-{auth.public_key}",
            "event_type": event.type,
            "time": _iso_to_epoch_ms(event.timestamp),
            "insert_id": event.id or str(uuid.uuid4()),
            "event_properties": {**event.body, "input_hash": input_hash},
            "user_properties": {
                "project_id": auth.public_key,
            },
            "platform": "llogr",
            "app_version": "0.1.0",
        })
    return amplitude_events


async def send_to_clickstream(
    events: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
    input_hash: str = "",
) -> None:
    """Send events to a Clickstream/Amplitude-compatible endpoint using POST /2/httpapi."""
    cfg = settings.clickstream
    amplitude_events = transform_to_amplitude(events, auth, input_hash=input_hash)

    payload = {
        "api_key": cfg.api_key,
        "events": amplitude_events,
        "options": {"min_id_length": 1},
    }

    max_retries = 3
    with CLICKSTREAM_FORWARD_SECONDS.time():
        for attempt in range(max_retries):
            try:
                verify = cfg.ca_bundle if cfg.ca_bundle else cfg.verify_ssl
                async with httpx.AsyncClient(verify=verify) as client:
                    resp = await client.post(
                        cfg.api_url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=10,
                    )
                    resp.raise_for_status()
                break
            except Exception:
                if attempt < max_retries - 1:
                    delay = 0.5 * (2 ** attempt)
                    logger.warning("clickstream_forward_retry", attempt=attempt + 1, delay=delay)
                    await asyncio.sleep(delay)
                else:
                    CLICKSTREAM_FORWARD_ERRORS.inc()
                    raise

    logger.info("forwarded_to_clickstream", events=len(amplitude_events))
