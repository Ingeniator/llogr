from __future__ import annotations

import base64
from typing import NamedTuple, Optional

import structlog
from fastapi import Header, HTTPException

logger = structlog.get_logger(__name__)


class AuthContext(NamedTuple):
    public_key: str
    secret_key: str


def get_auth(
    authorization: Optional[str] = Header(default=None),
    x_auth_tenant: Optional[str] = Header(default=None),
    x_auth_subject: Optional[str] = Header(default=None),
) -> AuthContext:
    """Extract auth from X-Auth-Tenant/Subject headers (nginx JWT) or Basic auth fallback."""
    # Prefer JWT claims forwarded by nginx
    if x_auth_tenant:
        logger.info("authenticated", public_key=x_auth_tenant, source="jwt")
        return AuthContext(public_key=x_auth_tenant, secret_key=x_auth_subject or "")

    # Fallback: Basic auth (e.g. Langfuse SDK calling directly)
    if not authorization or not authorization.startswith("Basic "):
        raise HTTPException(status_code=401, detail="Missing authentication")

    try:
        decoded = base64.b64decode(authorization.removeprefix("Basic ")).decode()
        public_key, secret_key = decoded.split(":", 1)
    except Exception:
        raise HTTPException(status_code=401, detail="Malformed credentials")

    if not public_key:
        raise HTTPException(status_code=401, detail="Empty credentials")

    logger.info("authenticated", public_key=public_key, source="basic")
    return AuthContext(public_key=public_key, secret_key=secret_key)
