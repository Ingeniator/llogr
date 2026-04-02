"""Full-text search endpoint — supports duckdb and clickhouse backends."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings
from llogr.s3 import list_batch_keys
from llogr.search import search_logs

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/api/public/logs/search")
async def search(
    q: str = Query(min_length=1),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    limit: int = Query(default=50, le=500),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
) -> dict:
    if not settings.features.search_enabled:
        raise HTTPException(status_code=404, detail="Search is not enabled")

    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    backend = settings.features.search_backend

    if backend == "clickhouse":
        from llogr.clickhouse import search_logs_ch

        results = await search_logs_ch(
            query=q,
            project_id=auth.public_key,
            settings=settings,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            limit=limit,
        )
        return {"results": results, "backend": "clickhouse"}

    # Default: duckdb — pre-filter keys by metadata, then scan content
    keys_meta = await list_batch_keys(
        auth, settings, start=start, end=end,
        session_id=session_id, trace_id=trace_id,
        trace_type=trace_type, input_hash=input_hash,
    )
    keys = [f["key"] for f in keys_meta]

    logger.info("search_scope", query=q, backend="duckdb", files=len(keys))

    if not keys:
        return {"results": [], "files_scanned": 0, "backend": "duckdb"}

    results = search_logs(keys, q, settings, limit=limit)

    return {"results": results, "files_scanned": len(keys), "keys": keys, "backend": "duckdb"}
