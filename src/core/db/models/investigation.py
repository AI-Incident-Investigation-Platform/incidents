from datetime import datetime
from uuid import UUID, uuid7

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Investigation(Base):
    __tablename__ = "investigations"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    incident_id: Mapped[UUID] = mapped_column(ForeignKey("incidents.id"))
    project_id: Mapped[UUID]
    attempt: Mapped[int]
    status: Mapped[str] = mapped_column(String(24), server_default="pending")
    snapshot: Mapped[dict] = mapped_column(JSONB)
    budget: Mapped[dict] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    context_note: Mapped[str | None]
    current_step: Mapped[int] = mapped_column(server_default="0")
    tool_calls_count: Mapped[int] = mapped_column(server_default="0")
    failure_code: Mapped[str | None] = mapped_column(String(64))
    failure_message: Mapped[str | None]
    admission_state: Mapped[str] = mapped_column(String(16), server_default="not_sent")
    lease_token: Mapped[UUID | None]
    lease_until: Mapped[datetime | None]
    next_start_at: Mapped[datetime | None]
    deadline: Mapped[datetime | None]
    cancel_requested_at: Mapped[datetime | None]
    cancel_delivered_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    parent_investigation_id: Mapped[UUID | None] = mapped_column(ForeignKey("investigations.id"))

    __table_args__ = (
        UniqueConstraint("incident_id", "attempt"),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        CheckConstraint("current_step >= 0", name="current_step_nonnegative"),
        CheckConstraint("tool_calls_count >= 0", name="tool_calls_nonnegative"),
        Index(
            "uq_investigations_active_incident",
            "incident_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'running')"),
        ),
    )
