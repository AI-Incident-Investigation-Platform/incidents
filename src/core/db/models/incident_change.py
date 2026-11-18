from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class IncidentChange(Base):
    __tablename__ = "incident_changes"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"), index=True)
    actor_id: Mapped[UUID]
    field: Mapped[str] = mapped_column(String(64))
    old_value: Mapped[dict | None] = mapped_column(JSONB)
    new_value: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
