from __future__ import annotations

import structlog

from llogr.auth import AuthContext
from llogr.config import get_settings
from llogr.metrics import EVENTS_INGESTED
from llogr.models import IngestionEvent
from llogr.s3 import save_batch_to_s3

logger = structlog.get_logger(__name__)


async def stage1_save_raw(
    batch: list[IngestionEvent], auth: AuthContext, session_id: str = "none",
) -> None:
    """Save raw events to S3."""
    settings = get_settings()
    key = await save_batch_to_s3(batch, auth, settings, session_id=session_id)
    EVENTS_INGESTED.labels(project_id=auth.public_key).inc(len(batch))
    logger.info("stage_1_complete", events=len(batch), key=key)


async def stage2_forward(batch: list[IngestionEvent], auth: AuthContext) -> None:
    """Forward events to configured backends (ClickHouse, ClickBeat, or both)."""
    settings = get_settings()
    forwarded_to = []

    # ClickHouse — direct insert
    if settings.clickhouse.enabled and settings.clickhouse.url:
        try:
            from llogr.clickhouse import insert_events
            await insert_events(batch, auth, settings)
            forwarded_to.append("clickhouse")
        except Exception:
            logger.exception("stage_2_clickhouse_failed")

    # ClickBeat — external forwarding
    if settings.clickbeat.enabled and settings.clickbeat.api_url:
        try:
            from llogr.clickbeat import send_to_clickbeat
            await send_to_clickbeat(batch, auth, settings)
            forwarded_to.append("clickbeat")
        except Exception:
            logger.exception("stage_2_clickbeat_failed")

    if forwarded_to:
        logger.info("stage_2_complete", events=len(batch), targets=forwarded_to)
