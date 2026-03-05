from __future__ import annotations

import logging

from llogr.auth import AuthContext
from llogr.clickbeat import send_to_clickbeat
from llogr.config import get_settings
from llogr.metrics import EVENTS_INGESTED
from llogr.models import IngestionEvent
from llogr.s3 import save_batch_to_s3

logger = logging.getLogger(__name__)


async def stage1_save_raw(
    batch: list[IngestionEvent], auth: AuthContext, session_id: str = "none",
) -> None:
    settings = get_settings()
    key = await save_batch_to_s3(batch, auth, settings, session_id=session_id)
    EVENTS_INGESTED.labels(project_id=auth.public_key).inc(len(batch))
    logger.info("Stage 1 complete: saved %d events to %s", len(batch), key)


async def stage2_forward_to_clickbeat(batch: list[IngestionEvent], auth: AuthContext) -> None:
    settings = get_settings()
    try:
        await send_to_clickbeat(batch, auth, settings)
        logger.info("Stage 2 complete: forwarded %d events to clickbeat", len(batch))
    except Exception:
        logger.exception("Stage 2 failed: error forwarding to clickbeat")
