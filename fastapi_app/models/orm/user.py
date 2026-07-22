from sqlalchemy import String, Integer, Boolean, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from fastapi_app.models.orm.base import Base
import datetime as dt


class User(Base):
    __tablename__ = "users"

    id:         Mapped[str]          = mapped_column(String(36), primary_key=True)
    name:       Mapped[str]          = mapped_column(String(100))
    email:      Mapped[str]          = mapped_column(String(255), unique=True, index=True)
    age:        Mapped[int]          = mapped_column(Integer)
    role:       Mapped[str]          = mapped_column(String(50), default="viewer")
    created_at: Mapped[dt.datetime]  = mapped_column(DateTime, default=dt.datetime.utcnow)

    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="user", lazy="selectin")
