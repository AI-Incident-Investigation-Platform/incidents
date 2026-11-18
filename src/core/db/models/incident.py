from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import Index, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID]
    environment_id: Mapped[UUID]
    display_number: Mapped[int]
    title: Mapped[str] = mapped_column(String(300))
    description: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(24), server_default="open")
    source: Mapped[str] = mapped_column(String(24))
    detected_at: Mapped[datetime]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now())
    resolved_at: Mapped[datetime | None]
    version: Mapped[int] = mapped_column(server_default="1")

    __table_args__ = (
        UniqueConstraint("project_id", "display_number"),
        Index("ix_incidents_project_status_detected", "project_id", "status", "detected_at", "id"),
    )
