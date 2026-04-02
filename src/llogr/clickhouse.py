"""ClickHouse search and ingestion backend."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx
import structlog

from llogr.auth import AuthContext
from llogr.config import ClickHouseConfig, Settings
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)

# Suppress httpx request logging — it leaks ClickHouse password in query params
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {database}.{table} (
    event_id     String,
    event_type   String,
    timestamp    DateTime64(3),
    project_id   String,
    model        String DEFAULT '',
    name         String DEFAULT '',
    trace_id     String DEFAULT '',
    session_id   String DEFAULT '',
    body         String,
    INDEX idx_body body TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
) ENGINE = MergeTree()
ORDER BY (project_id, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
"""


def _ch_url(cfg: ClickHouseConfig) -> str:
    return f"{cfg.url.rstrip('/')}/"


def _ch_params(cfg: ClickHouseConfig) -> dict:
    params = {"database": cfg.database}
    if cfg.user:
        params["user"] = cfg.user
    if cfg.password:
        params["password"] = cfg.password
    return params


async def ensure_table(settings: Settings) -> None:
    """Create the ClickHouse table if it doesn't exist."""
    cfg = settings.clickhouse
    if not cfg.url:
        return
    sql = CREATE_TABLE_SQL.format(database=cfg.database, table=cfg.table)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                _ch_url(cfg),
                params={**_ch_params(cfg), "query": sql},
            )
            resp.raise_for_status()
        logger.info("clickhouse_table_ready", table=f"{cfg.database}.{cfg.table}")
    except Exception as e:
        logger.error("clickhouse_ensure_table_failed", error=str(e))


async def insert_events(
    events: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
) -> None:
    """Insert events into ClickHouse."""
    cfg = settings.clickhouse
    if not cfg.url:
        return

    rows = []
    for ev in events:
        body = ev.body
        # Normalize timestamp to "YYYY-MM-DDTHH:MM:SS.mmm" (no timezone suffix)
        ts = datetime.fromisoformat(ev.timestamp).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        rows.append(json.dumps({
            "event_id": ev.id,
            "event_type": ev.type,
            "timestamp": ts,
            "project_id": auth.public_key,
            "model": body.get("model", "") or "",
            "name": body.get("name", "") or "",
            "trace_id": body.get("traceId", "") or "",
            "session_id": body.get("sessionId", "") or "",
            "body": json.dumps(body, default=str),
        }))

    data = "\n".join(rows)
    sql = f"INSERT INTO {cfg.database}.{cfg.table} FORMAT JSONEachRow"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                _ch_url(cfg),
                params={**_ch_params(cfg), "query": sql},
                content=data,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
    except Exception as e:
        logger.error("clickhouse_insert_failed", error=str(e))


async def search_logs_ch(
    query: str,
    project_id: str,
    settings: Settings,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Full-text search in ClickHouse."""
    cfg = settings.clickhouse
    if not cfg.url:
        return []

    conditions = ["project_id = {project_id:String}"]
    params = {"project_id": project_id}

    if start:
        conditions.append("timestamp >= {start:String}")
        params["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    if end:
        conditions.append("timestamp <= {end:String}")
        params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    if session_id:
        conditions.append("session_id = {session_id:String}")
        params["session_id"] = session_id
    if trace_id:
        conditions.append("trace_id = {trace_id:String}")
        params["trace_id"] = trace_id

    if query and query != "*":
        conditions.append("body ILIKE {query:String}")
        params["query"] = f"%{query}%"

    where = " AND ".join(conditions)
    sql = f"""
        SELECT event_id, event_type, timestamp, project_id, model, name, trace_id, session_id, body
        FROM {cfg.database}.{cfg.table}
        WHERE {where}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 500)}
        FORMAT JSON
    """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                _ch_url(cfg),
                params={
                    **_ch_params(cfg),
                    "query": sql,
                    **{f"param_{k}": v for k, v in params.items()},
                },
            )
            resp.raise_for_status()
            data = resp.json()

        results = []
        for row in data.get("data", []):
            try:
                body = json.loads(row.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            results.append({
                "id": row.get("event_id", ""),
                "type": row.get("event_type", ""),
                "timestamp": row.get("timestamp", ""),
                "body": body,
            })
        return results
    except Exception as e:
        logger.error("clickhouse_search_failed", error=str(e))
        return []
