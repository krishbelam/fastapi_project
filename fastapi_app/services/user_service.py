"""
services/user_service.py
────────────────────────
User business logic layer.
Routers call this — never touch DB directly from routes.
Session is always injected, never imported globally.
"""

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from fastapi_app.models.orm.user import User
from fastapi_app.models.user import UserCreate
from fastapi_app.core import settings

from logger_engine import logger

if not settings.POOL_PRE_PING:
    from tenacity import retry, retry_if_exception_type, stop_after_attempt
    from sqlalchemy.exc import OperationalError


async def create(db: AsyncSession, body: UserCreate) -> User:
    """
    Insert a new user.
    flush() assigns DB-generated values (id, created_at) without committing.
    Commit happens in get_db() after the request completes.
    """
    user = User(
        id=str(uuid.uuid4()),
        name=body.name,
        email=body.email,
        age=body.age,
        role=body.role,
        created_at=datetime.utcnow(),
    )
    db.add(user)
    await db.flush()  # makes id available without committing
    await db.refresh(user)  # sync ORM object with DB state
    logger.info(f"👤 User created: {user.id}")
    return user


# Smart alternative — disable pre_ping, rely on exception retry instead:
if settings.POOL_PRE_PING:
    async def get(db: AsyncSession, user_id: str) -> User | None:
        """
        Fetch a single user by ID.
        Returns None if not found — caller raises 404.
        """
        return await db.get(User, user_id)
else:
    print(
        "*" * 25,
        "\nSetting up the get method with retry!!!",
        f"\n{settings.POOL_PRE_PING=}",
        f"\n{settings.ROOT_DIR=}\n",
        "*" * 25
    )


    @retry(
        retry=retry_if_exception_type(OperationalError),
        stop=stop_after_attempt(2),  # retry once on stale connection
    )
    async def get(db: AsyncSession, user_id: str) -> User | None:
        """
        Fetch a single user by ID.
        Returns None if not found — caller raises 404.
        """
        return await db.get(User, user_id)


async def get_with_jobs(db: AsyncSession, user_id: str) -> User | None:
    """
    Fetch user with all their jobs eagerly loaded.
    Uses selectinload — async safe, avoids lazy load greenlet error.
    """
    result = await db.execute(
        select(User)
        .options(selectinload(User.jobs))
        .where(User.id == user_id)
    )
    return result.scalar_one_or_none()


async def list_all(db: AsyncSession) -> list[User]:
    """Fetch all users — add pagination for production."""
    result = await db.execute(select(User))
    return list(result.scalars().all())


async def update(db: AsyncSession, user_id: str, data: dict) -> User | None:
    """
    Partial update — only fields present in data dict are updated.
    Returns None if user not found.
    """
    user = await db.get(User, user_id)
    if not user:
        return None
    for field, value in data.items():
        if hasattr(user, field):
            setattr(user, field, value)
        else:
            logger.warinig(f"👤 User updated: Unknown field detected for the user id: {user_id}  field={field}")
    await db.flush()
    await db.refresh(user)
    logger.info(f"👤 User updated: {user_id}  fields={list(data.keys())}")
    return user


async def delete(db: AsyncSession, user_id: str) -> bool:
    """
    Delete user by ID.
    Returns True if deleted, False if not found.
    """
    user = await db.get(User, user_id)
    if not user:
        return False
    await db.delete(user)
    await db.flush()
    logger.info(f"👤 User deleted: {user_id}")
    return True
