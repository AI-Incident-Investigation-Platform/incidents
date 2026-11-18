from uuid import UUID, uuid7

from sqlalchemy import ForeignKeyConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.db.models.base import Base


class Evidence(Base):
    __tablename__ = "evidence"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    investigation_id: Mapped[UUID]
    hypothesis_id: Mapped[UUID | None]
    kind: Mapped[str] = mapped_column(String(32))
    title: Mapped[str]
    data: Mapped[dict] = mapped_column(JSONB)

    __table_args__ = (
        ForeignKeyConstraint(["investigation_id"], ["investigations.id"]),
        ForeignKeyConstraint(
            ["investigation_id", "hypothesis_id"], ["hypotheses.investigation_id", "hypotheses.id"]
        ),
        UniqueConstraint("investigation_id", "id"),
    )
