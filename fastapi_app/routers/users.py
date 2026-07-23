"""
routers/users.py
────────────────
User CRUD routes.
Routes are thin — all DB logic lives in services/user_service.py.
Session injected via Depends(get_db) — never imported globally.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi_app.rmq import publish
from fastapi_app.services import user_service
from fastapi_app.core import get_db, require_admin
from fastapi_app.models import UserCreate, UserResponse, UserUpdate

from fastapi_app.logger_engine import logger

router = APIRouter(prefix="/users", tags=["Users"])

# Type alias for cleaner signatures
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.post("/", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(body: UserCreate, db: DbDep):
    user = await user_service.create(db, body)
    logger.info(msg_from="User Create", msg="User created successfully, Now publishing the event to RMQ")
    await publish("user.created", {"id": user.id, "email": user.email, "role": user.role}
                  # , routing_key='task_queue'
                  )
    logger.info(msg_from="User Create", msg="User created event published to RMQ")
    return user


@router.get("/", response_model=list[UserResponse])
async def list_users(db: DbDep):
    return await user_service.list_all(db)


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(user_id: str, db: DbDep):
    user = await user_service.get(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.get("/{user_id}/with-jobs", response_model=UserResponse)
async def get_user_with_jobs(user_id: str, db: DbDep):
    """Returns user with all their jobs eagerly loaded."""
    user = await user_service.get_with_jobs(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(user_id: str, body: UserUpdate, db: DbDep):
    """
    Partial update — only provided fields are updated.
    PATCH not PUT — you don't need to send the full object.
    """
    update_data = body.to_update_dict()
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    user = await user_service.update(db, user_id, update_data)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await publish("user.updated", {"id": user_id, "fields": list(update_data.keys())})
    return user


@router.delete("/{user_id}", dependencies=[Depends(require_admin)], status_code=204)
async def delete_user(user_id: str, db: DbDep):
    """Admin-only. Returns 204 No Content on success."""
    deleted = await user_service.delete(db, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")
    await publish("user.deleted", {"id": user_id})