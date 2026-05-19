"""Fan-out: forward ingestion batches to external targets fire-and-forget."""
from __future__ import annotations

import base64

import httpx
import structlog

from llogr.auth import AuthContext
from llogr.config import ForwardTargetConfig
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)


async def forward_batch(
    batch: list[IngestionEvent],
    auth: AuthContext,
    target: ForwardTargetConfig,
) -> None:
    """POST batch to target URL. Errors are logged and swallowed — never propagated."""
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if target.pass_auth:
        creds = base64.b64encode(f"{auth.public_key}:{auth.secret_key}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    payload = {"batch": [e.model_dump() for e in batch]}
    url = f"{target.url.rstrip('/')}/api/public/ingestion"

    try:
        async with httpx.AsyncClient(timeout=target.timeout) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code >= 500:
                logger.warning("forward_batch_server_error", url=url, status=resp.status_code)
    except Exception as exc:
        logger.warning("forward_batch_failed", url=url, error=str(exc))
