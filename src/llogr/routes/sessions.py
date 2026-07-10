"""Sessions list and per-session trace drill-down."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get("/api/public/sessions")
async def list_sessions(
    start: datetime = Query(..., description="Window start (ISO 8601)"),
    end: datetime = Query(..., description="Window end (ISO 8601)"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")

    from llogr.clickhouse import list_sessions_ch
    return await list_sessions_ch(
        project_id=auth.public_key,
        settings=settings,
        start=start,
        end=end,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        limit=limit,
        offset=offset,
    )


@router.get("/api/public/sessions/{session_id}")
async def get_session_traces(
    session_id: str,
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    if settings.clickhouse.url:
        from llogr.clickhouse import get_session_traces_ch
        traces = await get_session_traces_ch(
            project_id=auth.public_key,
            settings=settings,
            session_id=session_id,
            is_org_admin=auth.is_org_admin,
            is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        )
        return {"session_id": session_id, "traces": traces}

    if settings.s3.bucket:
        from llogr.s3 import get_session_traces_s3
        traces = await get_session_traces_s3(
            session_id=session_id,
            auth=auth,
            settings=settings,
            is_org_admin=auth.is_org_admin,
            is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        )
        return {"session_id": session_id, "traces": traces}

    raise HTTPException(status_code=503, detail="No trace backend configured")


@router.get("/api/public/traces")
async def list_traces(
    agent_name: str | None = Query(default=None, description="Filter by agent name (name column)"),
    session_id: str | None = Query(default=None, description="Filter by session_id"),
    start: datetime | None = Query(default=None, description="Window start (ISO 8601)"),
    end: datetime | None = Query(default=None, description="Window end (ISO 8601)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    """Query traces by agent_name and/or session_id (both optional, combinable — AND)."""
    if not agent_name and not session_id:
        raise HTTPException(status_code=400, detail="At least one of agent_name or session_id is required")

    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if start and end and end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")

    from llogr.clickhouse import list_traces_ch
    traces = await list_traces_ch(
        project_id=auth.public_key,
        settings=settings,
        agent_name=agent_name,
        session_id=session_id,
        start=start,
        end=end,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        limit=limit,
        offset=offset,
    )
    return {"agent_name": agent_name, "session_id": session_id, "traces": traces}


@router.get("/api/public/agents")
async def list_agents(
    start: datetime | None = Query(default=None, description="Window start (ISO 8601); defaults to 7 days before end"),
    end: datetime | None = Query(default=None, description="Window end (ISO 8601); defaults to now"),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    """List known agent names (`x-agent-name` values). Mirrors Jaeger's GET /api/services."""
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    if end is None:
        end = datetime.now(timezone.utc)
    elif end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if start is None:
        start = end - timedelta(days=7)
    elif start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")

    from llogr.clickhouse import list_agent_names_ch
    names = await list_agent_names_ch(
        project_id=auth.public_key,
        settings=settings,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        start=start,
        end=end,
    )
    return {"data": names, "total": len(names), "limit": 0, "offset": 0, "errors": None}
