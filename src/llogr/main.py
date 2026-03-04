from __future__ import annotations

import logging

from fastapi import FastAPI

from llogr.routes.ingestion import router as ingestion_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="llogr", version="0.1.0")
app.include_router(ingestion_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
