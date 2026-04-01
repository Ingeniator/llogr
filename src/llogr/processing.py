from __future__ import annotations

import structlog

from llogr.auth import AuthContext
from llogr.config import get_settings
from llogr.metrics import EVENTS_INGESTED
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)


async def ingest(
    batch: list[IngestionEvent],
    auth: AuthContext,
    session_id: str = "none",
    request_id: str = "",
) -> list[str]:
    """Store events to all configured backends. Returns list of failed backend names."""
    settings = get_settings()
    backends = settings.features.store_backends
    stored_to = []
    failed = []

    if "s3" in backends:
        try:
            from llogr.s3 import save_batch_to_s3
            key = await save_batch_to_s3(batch, auth, settings, session_id=session_id, request_id=request_id)
            stored_to.append("s3")
            logger.info("stored_to_s3", key=key)
        except Exception:
            logger.exception("store_s3_failed")
            failed.append("s3")

    if "clickhouse" in backends and settings.clickhouse.url:
        try:
            from llogr.clickhouse import insert_events
            await insert_events(batch, auth, settings)
            stored_to.append("clickhouse")
        except Exception:
            logger.exception("store_clickhouse_failed")
            failed.append("clickhouse")

    if "clickstream" in backends and settings.clickstream.api_url:
        try:
            from llogr.clickstream import send_to_clickstream
            await send_to_clickstream(batch, auth, settings)
            stored_to.append("clickstream")
        except Exception:
            logger.exception("store_clickstream_failed")
            failed.append("clickstream")

    EVENTS_INGESTED.labels(project_id=auth.public_key).inc(len(batch))
    logger.info("ingest_complete", events=len(batch), targets=stored_to, failed=failed)
    return failed
