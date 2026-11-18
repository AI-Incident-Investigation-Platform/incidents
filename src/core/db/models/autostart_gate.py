from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class AutostartGate(Base):
    __tablename__ = "autostart_gates"

    project_id: Mapped[UUID] = mapped_column(primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"), primary_key=True)
    next_allowed_at: Mapped[datetime]
