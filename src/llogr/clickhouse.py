"""ClickHouse search and ingestion backend."""

from __future__ import annotations

import asyncio
import hashlib
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
    event_id      String,
    event_type    String,
    timestamp     DateTime64(3),
    project_id    String,
    model         String                    DEFAULT '',
    name          String                    DEFAULT '',
    trace_id      String                    DEFAULT '',
    session_id    String                    DEFAULT '',
    input_hash    String                    DEFAULT '',
    body          String,
    -- promoted fields (added via migrations for existing tables)
    start_time    DateTime64(3)             DEFAULT toDateTime64(0, 3),
    end_time      DateTime64(3)             DEFAULT toDateTime64(0, 3),
    duration_ms   Float64                   DEFAULT 0,
    provider      LowCardinality(String)    DEFAULT '',
    input_tokens  UInt32                    DEFAULT 0,
    output_tokens UInt32                    DEFAULT 0,
    total_tokens  UInt32                    DEFAULT 0,
    cost          Float64                   DEFAULT 0,
    input_cost    Float64                   DEFAULT 0,
    output_cost   Float64                   DEFAULT 0,
    finish_reason LowCardinality(String)    DEFAULT '',
    -- search / retrieval fields
    retrieval_query String                  DEFAULT '',
    result_count    UInt32                  DEFAULT 0,
    -- span linkage
    parent_span_id  String                  DEFAULT '',
    -- prompt identity: SHA-256[:8] of system messages (stable across turns on the same template)
    prompt_hash     String                  DEFAULT '',
    INDEX idx_body body TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
) ENGINE = MergeTree()
ORDER BY (project_id, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
"""

# Each entry is applied once via ALTER TABLE ADD COLUMN IF NOT EXISTS.
# Append-only — never remove or reorder rows.
_COLUMN_MIGRATIONS: list[tuple[str, str]] = [
    ("start_time",    "DateTime64(3)          DEFAULT toDateTime64(0, 3)"),
    ("end_time",      "DateTime64(3)          DEFAULT toDateTime64(0, 3)"),
    ("duration_ms",   "Float64                DEFAULT 0"),
    ("provider",      "LowCardinality(String) DEFAULT ''"),
    ("input_tokens",  "UInt32                 DEFAULT 0"),
    ("output_tokens", "UInt32                 DEFAULT 0"),
    ("total_tokens",  "UInt32                 DEFAULT 0"),
    ("cost",          "Float64                DEFAULT 0"),
    ("input_cost",    "Float64                DEFAULT 0"),
    ("output_cost",   "Float64                DEFAULT 0"),
    ("finish_reason",    "LowCardinality(String) DEFAULT ''"),
    ("retrieval_query",  "String                 DEFAULT ''"),
    ("result_count",     "UInt32                 DEFAULT 0"),
    ("parent_span_id",   "String                 DEFAULT ''"),
    ("prompt_hash",      "String                 DEFAULT ''"),
]


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
    """Create the ClickHouse table if it doesn't exist, then apply column migrations."""
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
        return

    # Apply any column additions that don't exist yet (idempotent).
    async with httpx.AsyncClient() as client:
        for col_name, col_def in _COLUMN_MIGRATIONS:
            alter = (
                f"ALTER TABLE {cfg.database}.{cfg.table} "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_def}"
            )
            try:
                resp = await client.post(_ch_url(cfg), params={**_ch_params(cfg), "query": alter})
                resp.raise_for_status()
            except Exception as e:
                logger.error("clickhouse_migration_failed", column=col_name, error=str(e))


# ---------------------------------------------------------------------------
# Field extraction helpers — all are safe against missing / wrong-typed data
# ---------------------------------------------------------------------------

def _parse_ts(value: str | None) -> str | None:
    """Return a ClickHouse-ready timestamp string (ms precision, no tz suffix) or None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    except (ValueError, TypeError):
        return None


def _extract_start_time(body: dict, fallback_ts: str) -> str:
    return _parse_ts(body.get("startTime")) or fallback_ts


def _extract_end_time(body: dict, fallback_ts: str) -> str:
    return _parse_ts(body.get("endTime")) or fallback_ts


def _extract_duration_ms(body: dict) -> float:
    meta = body.get("metadata")
    if isinstance(meta, dict):
        v = meta.get("duration_ms")
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    # Compute from start/end as fallback
    st = _parse_ts(body.get("startTime"))
    et = _parse_ts(body.get("endTime"))
    if st and et:
        try:
            delta = datetime.fromisoformat(et) - datetime.fromisoformat(st)
            return delta.total_seconds() * 1000
        except (ValueError, TypeError):
            pass
    return 0.0


def _extract_provider(body: dict) -> str:
    meta = body.get("metadata")
    if isinstance(meta, dict):
        return str(meta.get("provider") or "")
    return ""


def _extract_tokens(body: dict) -> tuple[int, int, int]:
    usage = body.get("usage")
    if not isinstance(usage, dict):
        usage = {}
    inp = int(usage.get("input") or 0)
    out = int(usage.get("output") or 0)
    tot = int(usage.get("total") or 0)
    if tot == 0 and (inp or out):
        tot = inp + out
    return inp, out, tot


def _extract_cost(body: dict) -> tuple[float, float, float]:
    """Return (input_cost, output_cost, total_cost) from a generation event body."""
    cd = body.get("costDetails")
    if isinstance(cd, dict):
        try:
            inp = float(cd["input"]) if cd.get("input") is not None else 0.0
            out = float(cd["output"]) if cd.get("output") is not None else 0.0
            tot_raw = cd.get("total")
            tot = float(tot_raw) if tot_raw is not None else inp + out
            if inp or out or tot:
                return inp, out, tot
        except (ValueError, TypeError):
            pass
    # Fallback: metadata.cost carries only the total (legacy yallmp format)
    meta = body.get("metadata")
    if isinstance(meta, dict):
        try:
            inp = float(meta["input_cost"]) if meta.get("input_cost") is not None else 0.0
            out = float(meta["output_cost"]) if meta.get("output_cost") is not None else 0.0
            tot_raw = meta.get("cost")
            tot = float(tot_raw) if tot_raw is not None else inp + out
            if inp or out or tot:
                return inp, out, tot
        except (ValueError, TypeError):
            pass
    return 0.0, 0.0, 0.0


def _extract_finish_reason(body: dict) -> str:
    output = body.get("output")
    if not isinstance(output, dict):
        return ""
    choices = output.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        return str(choices[0].get("finish_reason") or "")
    return ""


def _extract_retrieval_query(body: dict) -> str:
    """Extract a search/retrieval query from body.input.query (search spans)."""
    inp = body.get("input")
    if isinstance(inp, dict):
        q = inp.get("query")
        if q is not None:
            return str(q)
    return ""


def _extract_result_count(body: dict) -> int:
    """Extract search result count from body.output.result_count or metadata."""
    output = body.get("output")
    if isinstance(output, dict):
        v = output.get("result_count")
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    meta = body.get("metadata")
    if isinstance(meta, dict):
        v = meta.get("num_results_returned")
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return 0


def _extract_parent_span_id(body: dict) -> str:
    """Extract parent observation/span ID (present on child spans)."""
    return str(body.get("parentObservationId") or body.get("parentSpanId") or "")


def _extract_prompt_hash(body: dict) -> str:
    """SHA-256[:8] of system messages — stable 'prompt version' surrogate.

    Priority:
      1. System messages from body.input.messages  →  hash their role+content
      2. No system messages, but messages exist    →  hash model name (version proxy)
      3. No messages at all (search/span)          →  empty string
    """
    inp = body.get("input")
    if isinstance(inp, dict):
        messages = inp.get("messages")
        if isinstance(messages, list):
            system_msgs = [
                {"role": m["role"], "content": m.get("content", "")}
                for m in messages
                if isinstance(m, dict) and m.get("role") == "system"
            ]
            if system_msgs:
                raw = json.dumps(system_msgs, sort_keys=True, ensure_ascii=False)
                return hashlib.sha256(raw.encode()).hexdigest()[:8]
            # messages present but no system role → hash model as version proxy
            if messages:
                model = str(body.get("model") or "")
                if model:
                    return hashlib.sha256(model.encode()).hexdigest()[:8]
    return ""


async def insert_events(
    events: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
    input_hash: str = "",
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
        inp_tok, out_tok, tot_tok = _extract_tokens(body)
        inp_cost, out_cost, tot_cost = _extract_cost(body)
        rows.append(json.dumps({
            "event_id":     ev.id,
            "event_type":   ev.type,
            "timestamp":    ts,
            "project_id":   auth.public_key,
            "model":        body.get("model", "") or "",
            "name":         body.get("name", "") or "",
            "trace_id":     body.get("traceId", "") or "",
            "session_id":   body.get("sessionId", "") or "",
            "input_hash":   input_hash,
            "body":         json.dumps(body, default=str),
            # promoted fields
            "start_time":    _extract_start_time(body, ts),
            "end_time":      _extract_end_time(body, ts),
            "duration_ms":   _extract_duration_ms(body),
            "provider":      _extract_provider(body),
            "input_tokens":  inp_tok,
            "output_tokens": out_tok,
            "total_tokens":  tot_tok,
            "cost":          tot_cost,
            "input_cost":    inp_cost,
            "output_cost":   out_cost,
            "finish_reason":   _extract_finish_reason(body),
            "retrieval_query": _extract_retrieval_query(body),
            "result_count":    _extract_result_count(body),
            "parent_span_id":  _extract_parent_span_id(body),
            "prompt_hash":     _extract_prompt_hash(body),
        }))

    data = "\n".join(rows)
    sql = f"INSERT INTO {cfg.database}.{cfg.table} FORMAT JSONEachRow"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    _ch_url(cfg),
                    params={**_ch_params(cfg), "query": sql},
                    content=data,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
            return
        except Exception as e:
            if attempt < max_retries - 1:
                delay = 0.5 * (2 ** attempt)
                logger.warning("clickhouse_insert_retry", attempt=attempt + 1, delay=delay, error=str(e))
                await asyncio.sleep(delay)
            else:
                logger.error("clickhouse_insert_failed", error=str(e))


async def search_logs_ch(
    query: str,
    project_id: str,
    settings: Settings,
    is_org_admin: bool = False,
    is_whitelisted_agent: bool = False,
    is_super_admin: bool = False,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    trace_type: str | None = None,
    input_hash: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Full-text search in ClickHouse."""
    cfg = settings.clickhouse
    if not cfg.url:
        return []

    if is_super_admin or is_whitelisted_agent:
        conditions: list[str] = []
        params: dict[str, str] = {}
    elif is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = ["project_id LIKE {project_id:String}"]
        params = {"project_id": f"{org}/%"}
    else:
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
    if trace_type:
        conditions.append("name = {trace_type:String}")
        params["trace_type"] = trace_type
    if input_hash:
        conditions.append("input_hash = {input_hash:String}")
        params["input_hash"] = input_hash

    if query and query != "*":
        conditions.append("body ILIKE {query:String}")
        params["query"] = f"%{query}%"

    where = " AND ".join(conditions)
    sql = f"""
        SELECT event_id, event_type, timestamp, project_id, model, name, trace_id, session_id, input_hash, body
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
                "trace_id": row.get("trace_id", ""),
                "session_id": row.get("session_id", ""),
                "input_hash": row.get("input_hash", ""),
                "body": body,
            })
        return results
    except Exception as e:
        logger.error("clickhouse_search_failed", error=str(e))
        return []


async def export_generations_ch(
    project_id: str,
    settings: Settings,
    start: datetime,
    end: datetime,
    is_org_admin: bool = False,
    is_super_admin: bool = False,
    session_id: str | None = None,
):
    """Async generator: stream generation-create events as JSON lines."""
    cfg = settings.clickhouse
    if not cfg.url:
        return

    if is_super_admin:
        conditions: list[str] = []
        params: dict[str, str] = {}
    elif is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = ["project_id LIKE {project_id:String}"]
        params = {"project_id": f"{org}/%"}
    else:
        conditions = ["project_id = {project_id:String}"]
        params = {"project_id": project_id}

    # Include generation-update so that cross-batch streaming traces are exported.
    # When create+update arrive in the same batch they are already merged at ingestion
    # time; this catches the rare case where the update was flushed in a later batch.
    conditions.append("event_type IN ('generation-create', 'generation-update')")
    conditions.append("timestamp >= {start:String}")
    params["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    conditions.append("timestamp <= {end:String}")
    params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    if session_id:
        conditions.append("session_id = {session_id:String}")
        params["session_id"] = session_id

    where = " AND ".join(conditions)
    sql = (
        f"SELECT event_id, timestamp, project_id, model, name, trace_id, session_id, body "
        f"FROM {cfg.database}.{cfg.table} "
        f"WHERE {where} "
        f"ORDER BY timestamp ASC "
        f"FORMAT JSONEachRow"
    )

    ch_params = {**_ch_params(cfg), "query": sql}
    ch_params.update({f"param_{k}": v for k, v in params.items()})

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("POST", _ch_url(cfg), params=ch_params) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if line.strip():
                        yield line + "\n"
    except Exception as e:
        logger.error("clickhouse_export_failed", error=str(e))


async def get_billing_summary_ch(
    project_id: str,
    settings: Settings,
    is_org_admin: bool = False,
    is_super_admin: bool = False,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    user_limit: int = 500,
    user_offset: int = 0,
) -> dict:
    """Aggregate spend from ClickHouse for a billing period.

    Scope rules mirror yallmp's billing service:
      SUPER_ADMIN  – all orgs, all users
      ORG_ADMIN    – own org groups + per-user breakdown
      USER         – own org group total only, own user spend
    """
    cfg = settings.clickhouse
    if not cfg.url:
        return {"groups": [], "users": [], "current_user": None}

    start_str = period_start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    end_str = period_end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    org = project_id.split("/")[0] if project_id else ""

    # Base conditions shared by all queries
    base_conditions = [
        "cost > 0",
        "timestamp >= {start:String}",
        "timestamp < {end:String}",
    ]
    base_params: dict[str, str] = {"start": start_str, "end": end_str}

    # Org-scope condition (not applied for SUPER_ADMIN)
    if is_super_admin:
        scope_condition = ""
        scope_params: dict[str, str] = {}
    else:
        scope_condition = "(project_id LIKE {org_prefix:String} OR project_id = {org_exact:String})"
        scope_params = {"org_prefix": f"{org}/%", "org_exact": org}

    all_params = {**base_params, **scope_params}
    where_parts = base_conditions + ([scope_condition] if scope_condition else [])
    where = " AND ".join(where_parts)
    ch_query_params = {**_ch_params(cfg), **{f"param_{k}": v for k, v in all_params.items()}}

    # ── Groups query (per-org aggregation) ───────────────────────────────────
    groups_sql = f"""
        SELECT
            splitByChar('/', project_id)[1]  AS org,
            round(SUM(cost), 6)              AS group_spent,
            round(SUM(input_cost), 6)        AS input_spent,
            round(SUM(output_cost), 6)       AS output_spent,
            toUInt64(COUNT())                AS request_count
        FROM {cfg.database}.{cfg.table}
        WHERE {where}
        GROUP BY org
        ORDER BY group_spent DESC
        FORMAT JSON
    """

    # ── Users query (per-project_id, admins only) ────────────────────────────
    # Fetch one extra row to detect whether another page exists.
    users_sql = None
    if is_org_admin or is_super_admin:
        users_sql = f"""
            SELECT
                project_id,
                round(SUM(cost), 6)              AS user_spent,
                round(SUM(input_cost), 6)        AS input_spent,
                round(SUM(output_cost), 6)       AS output_spent,
                toUInt64(COUNT())                AS request_count
            FROM {cfg.database}.{cfg.table}
            WHERE {where}
            GROUP BY project_id
            ORDER BY user_spent DESC
            LIMIT {user_limit + 1} OFFSET {user_offset}
            FORMAT JSON
        """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(_ch_url(cfg), params={**ch_query_params, "query": groups_sql})
            resp.raise_for_status()
            groups = resp.json().get("data", [])

            users: list[dict] = []
            has_more = False
            if users_sql:
                resp = await client.post(_ch_url(cfg), params={**ch_query_params, "query": users_sql})
                resp.raise_for_status()
                raw_users = resp.json().get("data", [])
                has_more = len(raw_users) > user_limit
                users = raw_users[:user_limit]

        # ── Current user's own spend ──────────────────────────────────────────
        current_user = None
        if "/" in (project_id or ""):
            # Reuse users query result for admins; run targeted query for regular users
            for u in users:
                if u.get("project_id") == project_id:
                    current_user = u
                    break

            if current_user is None:
                cu_conditions = [
                    "cost > 0",
                    "timestamp >= {start:String}",
                    "timestamp < {end:String}",
                    "project_id = {project_id:String}",
                ]
                cu_sql = f"""
                    SELECT
                        project_id,
                        round(SUM(cost), 6)              AS user_spent,
                        round(SUM(input_cost), 6)        AS input_spent,
                        round(SUM(output_cost), 6)       AS output_spent,
                        toUInt64(COUNT())                AS request_count
                    FROM {cfg.database}.{cfg.table}
                    WHERE {" AND ".join(cu_conditions)}
                    GROUP BY project_id
                    FORMAT JSON
                """
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        _ch_url(cfg),
                        params={
                            **_ch_params(cfg),
                            "query": cu_sql,
                            "param_start": start_str,
                            "param_end": end_str,
                            "param_project_id": project_id,
                        },
                    )
                    resp.raise_for_status()
                    cu_rows = resp.json().get("data", [])
                    current_user = cu_rows[0] if cu_rows else None

        return {"groups": groups, "users": users, "has_more": has_more, "current_user": current_user}

    except Exception as exc:
        logger.error("clickhouse_billing_summary_failed", error=str(exc))
        return {"groups": [], "users": [], "has_more": False, "current_user": None}


async def list_sessions_ch(
    project_id: str,
    settings: Settings,
    start: datetime,
    end: datetime,
    is_org_admin: bool = False,
    is_super_admin: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Return a page of sessions with aggregated stats."""
    cfg = settings.clickhouse
    if not cfg.url:
        return {"sessions": []}

    if is_super_admin:
        conditions: list[str] = []
        params: dict[str, str] = {}
    elif is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = ["project_id LIKE {project_id:String}"]
        params = {"project_id": f"{org}/%"}
    else:
        conditions = ["project_id = {project_id:String}"]
        params = {"project_id": project_id}

    conditions += [
        "event_type IN ('generation-create', 'generation-update', 'span-create', 'span-update')",
        "session_id != ''",
        "timestamp >= {start:String}",
        "timestamp <= {end:String}",
    ]
    params["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    where = " AND ".join(conditions)
    sql = f"""
        SELECT
            session_id,
            project_id,
            toString(min(timestamp)) AS started_at,
            toString(max(timestamp)) AS last_event_at,
            count() AS event_count,
            groupUniqArray(model) AS models
        FROM {cfg.database}.{cfg.table}
        WHERE {where}
        GROUP BY session_id, project_id
        ORDER BY last_event_at DESC
        LIMIT {min(limit, 200)} OFFSET {offset}
        FORMAT JSON
    """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                _ch_url(cfg),
                params={**_ch_params(cfg), "query": sql, **{f"param_{k}": v for k, v in params.items()}},
            )
            resp.raise_for_status()
            data = resp.json()
        return {"sessions": data.get("data", [])}
    except Exception as e:
        logger.error("clickhouse_list_sessions_failed", error=str(e))
        return {"sessions": []}


async def get_session_traces_ch(
    project_id: str,
    settings: Settings,
    session_id: str,
    is_org_admin: bool = False,
    is_super_admin: bool = False,
) -> list[dict]:
    """Return all generation events for a single session."""
    cfg = settings.clickhouse
    if not cfg.url:
        return []

    if is_super_admin:
        conditions: list[str] = []
        params: dict[str, str] = {}
    elif is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = ["project_id LIKE {project_id:String}"]
        params = {"project_id": f"{org}/%"}
    else:
        conditions = ["project_id = {project_id:String}"]
        params = {"project_id": project_id}

    conditions += [
        "event_type IN ('generation-create', 'generation-update', 'span-create', 'span-update')",
        "session_id = {session_id:String}",
    ]
    params["session_id"] = session_id

    where = " AND ".join(conditions)
    sql = f"""
        SELECT event_id, timestamp, project_id, model, trace_id, session_id, body
        FROM {cfg.database}.{cfg.table}
        WHERE {where}
        ORDER BY timestamp ASC
        FORMAT JSON
    """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                _ch_url(cfg),
                params={**_ch_params(cfg), "query": sql, **{f"param_{k}": v for k, v in params.items()}},
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
                "event_id": row.get("event_id", ""),
                "timestamp": row.get("timestamp", ""),
                "project_id": row.get("project_id", ""),
                "model": row.get("model", ""),
                "session_id": row.get("session_id", ""),
                "trace_id": row.get("trace_id", ""),
                "body": body,
            })
        return results
    except Exception as e:
        logger.error("clickhouse_session_traces_failed", error=str(e))
        return []
