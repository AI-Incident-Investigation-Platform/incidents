from uuid import UUID, uuid7

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Hypothesis(Base):
    __tablename__ = "hypotheses"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    investigation_id: Mapped[UUID]
    slot: Mapped[int]
    title: Mapped[str] = mapped_column(String(300))
    explanation: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16))
    confidence: Mapped[str] = mapped_column(String(8))

    __table_args__ = (
        ForeignKeyConstraint(["investigation_id"], ["investigations.id"]),
        UniqueConstraint("investigation_id", "slot"),
        UniqueConstraint("investigation_id", "id"),
        CheckConstraint("slot BETWEEN 1 AND 3", name="slot_range"),
        CheckConstraint("confidence IN ('high', 'medium', 'low')", name="confidence_values"),
    )
