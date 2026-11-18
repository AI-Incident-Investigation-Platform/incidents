from datetime import datetime
from uuid import UUID

from sqlalchemy import func
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class ProjectTombstone(Base):
    __tablename__ = "project_tombstones"

    project_id: Mapped[UUID] = mapped_column(primary_key=True)
    lifecycle_version: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
