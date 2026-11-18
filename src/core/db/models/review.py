from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Review(Base):
    __tablename__ = "reviews"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"), index=True)
    investigation_id: Mapped[UUID] = mapped_column(ForeignKey("investigations.id"))
    reviewer_id: Mapped[UUID]
    decision: Mapped[str] = mapped_column(String(16))
    corrected_root_cause: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
