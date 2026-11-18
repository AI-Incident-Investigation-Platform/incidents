from datetime import date
from uuid import UUID

from sqlalchemy import CheckConstraint, String
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class QuotaCounter(Base):
    __tablename__ = "quota_counters"

    project_id: Mapped[UUID] = mapped_column(primary_key=True)
    day: Mapped[date] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), primary_key=True)
    used: Mapped[int] = mapped_column(server_default="0")

    __table_args__ = (CheckConstraint("used >= 0", name="used_nonnegative"),)
