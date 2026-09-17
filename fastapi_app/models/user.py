"""
models/user.py
──────────────
Pydantic v2 models for the User domain.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, computed_field, field_validator, model_validator


class UserBase(BaseModel):
    """Shared validators live here once."""
    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("email", check_fields=False)
    @classmethod
    def email_must_have_at(cls, v: str | None) -> str | None:
        if v is not None and "@" not in v:
            raise ValueError("Invalid email address")
        return v.lower() if v else v

    @field_validator("age", check_fields=False)
    @classmethod
    def age_must_be_positive(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            raise ValueError("Age must be positive")
        return v


class UserCreate(UserBase):
    """All fields required on create."""
    name: str
    email: str
    age: int
    role: str = "viewer"

    @model_validator(mode="after")
    def admin_must_be_adult(self) -> "UserCreate":
        if self.role == "admin" and self.age < 18:
            raise ValueError("Admin users must be 18+")
        return self


class UserResponse(UserBase):
    id: str
    name: str
    email: str
    age: int
    role: str
    created_at: datetime

    @computed_field
    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


class UserUpdate(UserBase):
    """All fields optional on update — validators inherited from UserBase."""
    name: str | None = None
    email: str | None = None
    age: int | None = None
    role: str | None = None

    def to_update_dict(self) -> dict:
        return {k: v for k, v in self.model_dump().items() if v is not None}
