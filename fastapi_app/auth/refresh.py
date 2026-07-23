import secrets
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi_app.models.orm import RefreshToken
from fastapi_app.core import hash_token

REFRESH_TOKEN_EXPIRE_DAYS = 30


async def rotate_refresh_token(
        old_token: str,
        db: AsyncSession
) -> str:
    """
    Validate existing refresh token and rotate it.

    Flow:
    1. Hash incoming raw refresh token.
    2. Find token in DB.
    3. Validate token status.
    4. Revoke old token.
    5. Create and return new raw token.
    """

    old_token_hash = hash_token(old_token)

    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token_hash == old_token_hash
        )
    )

    token_row = result.scalar_one_or_none()

    if not token_row:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token"
        )

    if token_row.revoked:
        raise HTTPException(
            status_code=401,
            detail="Refresh token revoked"
        )

    if token_row.expires_at < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=401,
            detail="Refresh token expired"
        )

    # revoke old token
    token_row.revoked = True

    # create new refresh token
    new_raw_token = secrets.token_urlsafe(64)

    new_token_hash = hash_token(new_raw_token)

    new_refresh_token = RefreshToken(
        token_hash=new_token_hash,
        user_id=token_row.user_id,
        expires_at=datetime.now(timezone.utc)
                   + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        revoked=False,
    )

    db.add(new_refresh_token)

    await db.commit()

    return new_raw_token


async def create_refresh_token(
        user_id: int,
        db: AsyncSession
) -> str:
    raw_token = secrets.token_urlsafe(64)

    token_hash = hash_token(raw_token)

    refresh_token = RefreshToken(
        token_hash=token_hash,
        user_id=user_id,
        expires_at=datetime.now(timezone.utc)
                   + timedelta(days=30),
    )

    db.add(refresh_token)

    await db.commit()

    return raw_token
