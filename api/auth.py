from __future__ import annotations

import hmac
import logging
from typing import Optional

from fastapi import Header, HTTPException, WebSocket, status

from langmonitor.config import settings

log = logging.getLogger(__name__)


def _matches(provided: Optional[str], expected: str) -> bool:
    if not provided:
        return False
    # Constant-time comparison to avoid leaking the key via timing.
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


async def require_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """FastAPI dependency enforcing the X-API-Key header on REST routes.

    When ``API_KEY`` is unset the server runs in (development) open mode and the
    dependency is a no-op — the loud startup warning covers that case.
    """
    expected = settings.API_KEY
    if not expected:
        return
    if not _matches(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


def websocket_authorized(websocket: WebSocket) -> bool:
    """Authorise a WebSocket handshake.

    The key may arrive as the ``X-API-Key`` header (used by the SDK) or an
    ``api_key`` query parameter (browser dashboards that can't set headers).
    """
    expected = settings.API_KEY
    if not expected:
        return True
    provided = websocket.headers.get("x-api-key") or websocket.query_params.get(
        "api_key"
    )
    return _matches(provided, expected)
