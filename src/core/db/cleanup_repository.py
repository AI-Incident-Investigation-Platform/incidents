from uuid import UUID

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import ProjectTombstone

TABLE_FILTERS = (
    (
        "evidence",
        "JOIN investigations i ON t.investigation_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "timeline_entries",
        "JOIN incidents i ON t.incident_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "hypotheses",
        "JOIN investigations i ON t.investigation_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "rca_reports",
        "JOIN investigations i ON t.investigation_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "investigation_results",
        "JOIN investigations i ON t.investigation_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "investigation_events",
        "JOIN investigations i ON t.investigation_id = i.id WHERE i.project_id = :project_id",
    ),
    ("reviews", "JOIN incidents i ON t.incident_id = i.id WHERE i.project_id = :project_id"),
    ("investigations", "WHERE t.project_id = :project_id"),
    (
        "incident_changes",
        "JOIN incidents i ON t.incident_id = i.id WHERE i.project_id = :project_id",
    ),
    (
        "incident_services",
        "JOIN incidents i ON t.incident_id = i.id WHERE i.project_id = :project_id",
    ),
    ("alerts", "WHERE t.project_id = :project_id"),
    ("ingest_receipts", "WHERE t.project_id = :project_id"),
    ("correlation_slots", "WHERE t.project_id = :project_id"),
    ("autostart_gates", "WHERE t.project_id = :project_id"),
    ("incidents", "WHERE t.project_id = :project_id"),
    ("project_configs", "WHERE t.project_id = :project_id"),
    ("quota_counters", "WHERE t.project_id = :project_id"),
    ("idempotency_keys", "WHERE t.project_id = :project_id"),
    ("audit_events", "WHERE t.project_id = :project_id"),
    (
        "outbox",
        "WHERE t.body ->> 'project_id' = :project_id_text "
        "AND t.body ->> 'type' <> 'cleanup.completed'",
    ),
)


class CleanupRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def mark_deleting(self, project_id: UUID, lifecycle_version: int) -> None:
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:project_id))"),
            {"project_id": f"incidents:{project_id}"},
        )
        await self.session.execute(
            insert(ProjectTombstone)
            .values(project_id=project_id, lifecycle_version=lifecycle_version)
            .on_conflict_do_update(
                index_elements=["project_id"],
                set_={
                    "lifecycle_version": func.greatest(
                        ProjectTombstone.lifecycle_version, lifecycle_version
                    )
                },
            )
        )

    async def delete_batch(
        self, table: str, predicate: str, project_id: UUID, limit: int = 500
    ) -> int:
        if (table, predicate) not in TABLE_FILTERS:
            raise ValueError("Unknown cleanup table")
        statement = text(
            f"DELETE FROM {table} WHERE ctid IN ("
            f"SELECT t.ctid FROM {table} AS t {predicate} LIMIT :limit)"
        )
        result = await self.session.execute(
            statement,
            {"project_id": project_id, "project_id_text": str(project_id), "limit": limit},
        )
        return int(getattr(result, "rowcount", 0) or 0)
