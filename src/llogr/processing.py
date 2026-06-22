from __future__ import annotations

import asyncio
import structlog

from llogr.auth import AuthContext
from llogr.config import get_settings
from llogr.metrics import EVENTS_INGESTED
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)

# event types where updates should be merged back into the corresponding create
_MERGE_PAIRS: dict[str, str] = {
    "generation-update": "generation-create",
    "span-update": "span-create",
}


def _merge_update_events(batch: list[IngestionEvent]) -> list[IngestionEvent]:
    """Merge *-update events into corresponding *-create events in the same batch.

    Langfuse SDK streaming pattern sends a generation-create (input only, no
    output) followed by a generation-update (full output + usage) in the same
    flush batch.  Without merging, the create row stored in ClickHouse has no
    output, and queries that filter on event_type='generation-create' miss the
    actual LLM response.

    Merging is keyed on body["id"] (the Langfuse observation/span ID, which is
    the same in both the create and the update event).  Fields from the update
    overwrite those in the create; the "id" key itself is never overwritten.
    Update events that have no matching create in this batch are kept as-is.
    """
    creates: dict[str, IngestionEvent] = {}  # body.id → create event
    update_groups: dict[str, list[IngestionEvent]] = {}  # body.id → [updates]
    other: list[IngestionEvent] = []

    for event in batch:
        body_id: str = event.body.get("id", "")
        target_type = _MERGE_PAIRS.get(event.type)
        if target_type is not None:
            # This is an *-update event
            if body_id:
                update_groups.setdefault(body_id, []).append(event)
            else:
                other.append(event)
        elif event.type in _MERGE_PAIRS.values():
            # This is a *-create event
            if body_id:
                creates[body_id] = event
            else:
                other.append(event)
        else:
            other.append(event)

    standalone_updates: list[IngestionEvent] = []
    for body_id, updates in update_groups.items():
        if body_id in creates:
            create_ev = creates[body_id]
            for update_ev in updates:
                for key, value in update_ev.body.items():
                    if key != "id" and value is not None:
                        create_ev.body[key] = value
            logger.debug(
                "merged_generation_update",
                body_id=body_id,
                updates=len(updates),
            )
        else:
            standalone_updates.extend(updates)

    return list(creates.values()) + standalone_updates + other


async def ingest(
    batch: list[IngestionEvent],
    auth: AuthContext,
    session_id: str = "",
    request_id: str = "",
    agent_name: str = "",
) -> list[str]:
    """Store events to all configured backends. Returns list of failed backend names."""
    settings = get_settings()
    backends = settings.features.store_backends
    stored_to = []
    failed = []

    # Merge generation-update / span-update into their create counterparts so
    # that streaming traces (which arrive as separate create + update events)
    # are stored as a single complete row in every backend.
    batch = _merge_update_events(batch)

    # Stamp fallback session_id onto events that don't carry one
    if session_id:
        for event in batch:
            if not event.body.get("sessionId"):
                event.body["sessionId"] = session_id

    # Stamp agent_name onto events that have no name (or OTEL's "unknown" fallback)
    if agent_name:
        for event in batch:
            name = event.body.get("name")
            if not name or name == "unknown":
                event.body["name"] = agent_name

    # Pre-compute shared metadata once
    from llogr.s3 import extract_input_hash
    input_hash = extract_input_hash(batch)

    if "s3" in backends:
        try:
            from llogr.s3 import save_batch_to_s3
            key = await save_batch_to_s3(batch, auth, settings, session_id=session_id, request_id=request_id, input_hash=input_hash)
            stored_to.append("s3")
            logger.info("stored_to_s3", key=key)
        except Exception:
            logger.exception("store_s3_failed")
            failed.append("s3")

    if "clickhouse" in backends and settings.clickhouse.url:
        try:
            from llogr.clickhouse import insert_events
            await insert_events(batch, auth, settings, input_hash=input_hash)
            stored_to.append("clickhouse")
        except Exception:
            logger.exception("store_clickhouse_failed")
            failed.append("clickhouse")

    if "clickstream" in backends and settings.clickstream.api_url:
        try:
            from llogr.clickstream import send_to_clickstream
            await send_to_clickstream(batch, auth, settings, input_hash=input_hash)
            stored_to.append("clickstream")
        except Exception:
            logger.exception("store_clickstream_failed")
            failed.append("clickstream")

    for target in settings.features.forward:
        from llogr.forward import forward_batch
        asyncio.create_task(forward_batch(batch, auth, target))

    EVENTS_INGESTED.labels(project_id=auth.public_key).inc(len(batch))
    logger.info("ingest_complete", events=len(batch), targets=stored_to, failed=failed)
    return failed
