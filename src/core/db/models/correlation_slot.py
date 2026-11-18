from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class CorrelationSlot(Base):
    __tablename__ = "correlation_slots"

    project_id: Mapped[UUID] = mapped_column(primary_key=True)
    correlation_key: Mapped[str] = mapped_column(String(500), primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"))
    episode_until: Mapped[datetime]
