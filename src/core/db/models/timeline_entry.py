from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKeyConstraint, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class TimelineEntry(Base):
    __tablename__ = "timeline_entries"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    incident_id: Mapped[UUID]
    investigation_id: Mapped[UUID | None]
    event_type: Mapped[str] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(300))
    description: Mapped[str] = mapped_column(Text)
    service_ids: Mapped[list] = mapped_column(JSONB)
    source: Mapped[dict] = mapped_column(JSONB)
    causality: Mapped[str] = mapped_column(String(24))
    occurred_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["incident_id"], ["incidents.id"]),
        ForeignKeyConstraint(["investigation_id"], ["investigations.id"]),
    )
