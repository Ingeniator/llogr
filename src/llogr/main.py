from __future__ import annotations

import structlog
from fastapi import FastAPI, Request
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import latency, request_size, requests, response_size
from starlette.middleware.base import BaseHTTPMiddleware

from llogr.config import get_settings
from llogr.routes.ingestion import router as ingestion_router
from llogr.routes.logs import router as logs_router
from llogr.routes.otel import router as otel_router
from llogr.routes.search import router as search_router
from llogr.routes.ui import router as ui_router

settings = get_settings()
app = FastAPI(title="llogr", version="0.1.0", root_path=settings.server.root_path)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id", "")
        structlog.contextvars.clear_contextvars()
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)
        return await call_next(request)


app.add_middleware(RequestIDMiddleware)


@app.on_event("startup")
async def _startup():
    if "s3" in settings.features.store_backends:
        from llogr.s3 import ensure_bucket
        await ensure_bucket(settings)

    needs_ch = (
        "clickhouse" in settings.features.store_backends
        or settings.features.search_backend == "clickhouse"
    )
    if needs_ch and settings.clickhouse.url:
        from llogr.clickhouse import ensure_table
        await ensure_table(settings)
app.include_router(ingestion_router)
app.include_router(otel_router)
app.include_router(logs_router)
app.include_router(search_router)
app.include_router(ui_router)


Instrumentator(
    should_group_status_codes=False,
    should_group_untemplated=True,
).add(
    latency(),
).add(
    request_size(),
).add(
    response_size(),
).add(
    requests(),
).instrument(app).expose(app)


@app.get("/livez")
async def livez() -> dict:
    """Liveness probe — process is alive, no dependency checks."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness probe — returns 200 if all storage backends are healthy, 503 otherwise."""
    import asyncio

    import aioboto3
    import httpx as _httpx
    from starlette.responses import Response as StarletteResponse

    # --- S3 / MinIO ---
    if "s3" in settings.features.store_backends:
        try:
            s3_cfg = settings.s3
            session = aioboto3.Session(
                aws_access_key_id=s3_cfg.access_key_id,
                aws_secret_access_key=s3_cfg.secret_access_key,
                region_name=s3_cfg.region,
            )
            async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
                await asyncio.wait_for(
                    client.head_bucket(Bucket=s3_cfg.bucket),
                    timeout=3,
                )
        except Exception:
            return StarletteResponse(status_code=503)

    # --- ClickHouse ---
    needs_ch = (
        "clickhouse" in settings.features.store_backends
        or settings.features.search_backend == "clickhouse"
    )
    if needs_ch and settings.clickhouse.url:
        try:
            async with _httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(
                    f"{settings.clickhouse.url.rstrip('/')}/ping",
                )
                resp.raise_for_status()
        except Exception:
            return StarletteResponse(status_code=503)

    return StarletteResponse(status_code=200)


@app.get("/health")
async def health() -> dict:
    """Full health status with component details — for dashboards and monitoring."""
    import asyncio

    import aioboto3
    import httpx as _httpx

    components: dict[str, str] = {}
    details: dict[str, str] = {}

    # --- S3 / MinIO ---
    if "s3" in settings.features.store_backends:
        try:
            s3_cfg = settings.s3
            session = aioboto3.Session(
                aws_access_key_id=s3_cfg.access_key_id,
                aws_secret_access_key=s3_cfg.secret_access_key,
                region_name=s3_cfg.region,
            )
            async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
                await asyncio.wait_for(
                    client.head_bucket(Bucket=s3_cfg.bucket),
                    timeout=3,
                )
            components["s3"] = "ok"
        except Exception as exc:
            components["s3"] = "degraded"
            details["s3"] = str(exc)
    else:
        components["s3"] = "disabled"

    # --- ClickHouse ---
    needs_ch = (
        "clickhouse" in settings.features.store_backends
        or settings.features.search_backend == "clickhouse"
    )
    if needs_ch and settings.clickhouse.url:
        try:
            async with _httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(
                    f"{settings.clickhouse.url.rstrip('/')}/ping",
                )
                resp.raise_for_status()
            components["clickhouse"] = "ok"
        except Exception as exc:
            components["clickhouse"] = "degraded"
            details["clickhouse"] = str(exc)
    else:
        components["clickhouse"] = "disabled"

    enabled = {k: v for k, v in components.items() if v != "disabled"}
    status = "ok" if all(v == "ok" for v in enabled.values()) else "degraded"

    result: dict = {"status": status, "components": components}
    if details:
        result["details"] = details
    return result


@app.get("/api/public/ui-config")
def ui_config() -> dict:
    return {
        "search_enabled": settings.features.search_enabled,
        "search_backend": settings.features.search_backend if settings.features.search_enabled else None,
    }
