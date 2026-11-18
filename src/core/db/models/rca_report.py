from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class RcaReport(Base):
    __tablename__ = "rca_reports"

    investigation_id: Mapped[UUID] = mapped_column(
        ForeignKey("investigations.id"), primary_key=True
    )
    summary: Mapped[str]
    root_cause: Mapped[str | None]
    recommendations: Mapped[list] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
