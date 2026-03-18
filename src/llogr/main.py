from __future__ import annotations

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import latency, request_size, requests, response_size

from llogr.config import get_settings
from llogr.routes.ingestion import router as ingestion_router
from llogr.routes.logs import router as logs_router
from llogr.routes.otel import router as otel_router
from llogr.routes.search import router as search_router
from llogr.routes.ui import router as ui_router

settings = get_settings()
app = FastAPI(title="llogr", version="0.1.0", root_path=settings.server.root_path)


@app.on_event("startup")
async def _startup():
    if settings.clickhouse.enabled and settings.clickhouse.url:
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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/public/ui-config")
def ui_config() -> dict:
    return {
        "search_enabled": settings.features.search_enabled,
        "search_backend": settings.features.search_backend if settings.features.search_enabled else None,
    }
