from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class IncidentService(Base):
    __tablename__ = "incident_services"

    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"), primary_key=True)
    service_id: Mapped[UUID] = mapped_column(primary_key=True)
    name_snapshot: Mapped[str] = mapped_column(String(200))
