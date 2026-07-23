"""
core/dependencies.py
────────────────────
All FastAPI Depends() functions in one place.
Import these into routers — never define DI inside route files.
"""

from typing import Annotated

import httpx
from fastapi import Depends, HTTPException, Request


# ── Shared HTTP client ────────────────────────────────────────────────────────
async def get_http_client(request: Request) -> httpx.AsyncClient:
    """
    Returns the shared httpx.AsyncClient stored on app.state during lifespan.
    One connection pool for the whole process — never create per-request clients.
    """
    return request.app.state.http_client


HttpClientDep = Annotated[httpx.AsyncClient, Depends(get_http_client)]


# ── Auth ──────────────────────────────────────────────────────────────────────
# NOTE: fake_users_db is imported here to keep routers decoupled from storage.
# Replace this with a real DB session dependency (SQLAlchemy async session, etc.)
# from core.store import fake_users_db   # noqa: E402  (circular-safe at runtime)


def get_current_user(user_id: str) -> dict:
    """
    Resolves a user from the path parameter user_id.
    Replace with: decode JWT → look up in DB.
    """
    from core.store import fake_users_db  # noqa: E402  (circular-safe at runtime)

    user = fake_users_db.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def require_admin(user: Annotated[dict, Depends(get_current_user)]) -> dict:
    """
    Composed dependency — calls get_current_user, then enforces admin role.
    Usage:  dependencies=[Depends(require_admin)]  on a route.
    """
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user


CurrentUserDep = Annotated[dict, Depends(get_current_user)]
AdminDep       = Annotated[dict, Depends(require_admin)]
