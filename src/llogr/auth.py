from __future__ import annotations

import base64
import logging
from typing import NamedTuple

from fastapi import Header, HTTPException

logger = logging.getLogger(__name__)


class AuthContext(NamedTuple):
    public_key: str
    secret_key: str


def get_auth(authorization: str = Header()) -> AuthContext:
    """Extract and validate Basic auth credentials."""
    if not authorization.startswith("Basic "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    try:
        decoded = base64.b64decode(authorization.removeprefix("Basic ")).decode()
        public_key, secret_key = decoded.split(":", 1)
    except Exception:
        raise HTTPException(status_code=401, detail="Malformed credentials")

    if not public_key or not secret_key:
        raise HTTPException(status_code=401, detail="Empty credentials")

    logger.info("Authenticated request from %s", public_key)
    return AuthContext(public_key=public_key, secret_key=secret_key)
