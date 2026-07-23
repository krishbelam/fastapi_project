from typing import Annotated

from fastapi import Depends
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordBearer

from sqlalchemy.ext.asyncio import AsyncSession

from fastapi_app.auth._jwt import decode_token
from fastapi_app.core import get_db
from fastapi_app.models.orm import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


async def get_current_user(
        token: Annotated[str, Depends(oauth2_scheme)],
        db: AsyncSession = Depends(get_db),
) -> User:
    payload = decode_token(token)
    user = await db.get(User, payload["sub"])
    if not user:
        raise HTTPException(401, "User not found")
    return user
