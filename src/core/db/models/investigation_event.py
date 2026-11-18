from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class InvestigationEvent(Base):
    __tablename__ = "investigation_events"

    investigation_id: Mapped[UUID] = mapped_column(
        ForeignKey("investigations.id"), primary_key=True
    )
    sequence: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[UUID] = mapped_column(unique=True)
    kind: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSONB)
    occurred_at: Mapped[datetime]
