from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class IngestReceipt(Base):
    __tablename__ = "ingest_receipts"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    project_id: Mapped[UUID] = mapped_column(index=True)
    token_id: Mapped[UUID]
    kind: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), server_default="queued")
    reason: Mapped[str | None] = mapped_column(String(200))
    resource_id: Mapped[UUID | None]
    incident_id: Mapped[UUID | None] = mapped_column(ForeignKey("incidents.id"))
    received_at: Mapped[datetime] = mapped_column(server_default=func.now())
    processed_at: Mapped[datetime | None]

    __table_args__ = (Index("ix_ingest_receipts_received_at", "received_at"),)
