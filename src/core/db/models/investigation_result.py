from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class InvestigationResult(Base):
    __tablename__ = "investigation_results"

    investigation_id: Mapped[UUID] = mapped_column(
        ForeignKey("investigations.id"), primary_key=True
    )
    status: Mapped[str] = mapped_column(String(24))
    payload: Mapped[dict] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(server_default=func.now())
