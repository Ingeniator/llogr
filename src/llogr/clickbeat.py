from __future__ import annotations

import httpx
import structlog

from llogr.auth import AuthContext
from llogr.config import Settings
from llogr.metrics import CLICKBEAT_FORWARD_ERRORS, CLICKBEAT_FORWARD_SECONDS
from llogr.models import IngestionEvent

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
