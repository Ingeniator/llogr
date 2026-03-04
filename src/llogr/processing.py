from __future__ import annotations

import json
import logging

from llogr.auth import AuthContext
from llogr.models import IngestionEvent

logger = logging.getLogger(__name__)


def process_events(events: list[IngestionEvent], auth: AuthContext) -> None:
    """Process ingestion events. Currently logs to console."""
    for event in events:
        logger.info(
            "event | project=%s type=%s id=%s body=%s",
            auth.public_key,
            event.type,
            event.id,
            json.dumps(event.body, default=str),
        )
