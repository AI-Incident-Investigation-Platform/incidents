from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID]
    environment_id: Mapped[UUID]
    token_id: Mapped[UUID]
    source: Mapped[str] = mapped_column(String(200))
    source_event_id: Mapped[str] = mapped_column(String(500))
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSONB)
    incident_id: Mapped[UUID | None] = mapped_column(ForeignKey("incidents.id"))
    started_at: Mapped[datetime]
    received_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __table_args__ = (
        UniqueConstraint("project_id", "source", "source_event_id"),
        Index("ix_alerts_project_received", "project_id", "received_at"),
    )
