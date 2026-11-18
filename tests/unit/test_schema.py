import pytest
from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from src.core.db.models import Base


@pytest.mark.case(id="INC-DB-001")
def test_incident_schema_contains_all_owned_tables() -> None:
    expected = {
        "inbox",
        "outbox",
        "ingest_receipts",
        "alerts",
        "correlation_slots",
        "project_configs",
        "incidents",
        "incident_services",
        "incident_changes",
        "investigations",
        "investigation_events",
        "investigation_results",
        "rca_reports",
        "hypotheses",
        "evidence",
        "timeline_entries",
        "reviews",
        "autostart_gates",
        "quota_counters",
        "idempotency_keys",
        "audit_events",
    }
    assert expected <= set(Base.metadata.tables)


@pytest.mark.case(id="INC-DB-002")
def test_alert_deduplication_is_scoped_to_project() -> None:
    alerts = Base.metadata.tables["alerts"]
    columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in alerts.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("project_id", "source", "source_event_id") in columns


@pytest.mark.case(id="INC-DB-003")
def test_only_one_active_investigation_per_incident() -> None:
    investigations = Base.metadata.tables["investigations"]
    assert any(
        isinstance(index, Index)
        and index.unique
        and [column.name for column in index.columns] == ["incident_id"]
        and index.dialect_options["postgresql"].get("where") is not None
        for index in investigations.indexes
    )


@pytest.mark.case(id="INC-DB-004")
def test_hypothesis_slots_are_limited_to_three() -> None:
    hypotheses = Base.metadata.tables["hypotheses"]
    assert any(
        isinstance(constraint, CheckConstraint)
        and "slot" in str(constraint.sqltext)
        and "3" in str(constraint.sqltext)
        for constraint in hypotheses.constraints
    )
