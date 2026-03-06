from __future__ import annotations

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import latency, request_size, requests, response_size

from llogr.config import get_settings
from llogr.routes.ingestion import router as ingestion_router
from llogr.routes.logs import router as logs_router
from llogr.routes.ui import router as ui_router

settings = get_settings()
app = FastAPI(title="llogr", version="0.1.0", root_path=settings.server.root_path)
app.include_router(ingestion_router)
app.include_router(logs_router)
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
