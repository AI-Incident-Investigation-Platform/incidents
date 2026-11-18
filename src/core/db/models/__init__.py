from aiip_kit.outbox.tables import inbox_table, outbox_table

from src.core.db.models.alert import Alert
from src.core.db.models.audit_event import AuditEvent
from src.core.db.models.autostart_gate import AutostartGate
from src.core.db.models.base import Base
from src.core.db.models.correlation_slot import CorrelationSlot
from src.core.db.models.evidence import Evidence
from src.core.db.models.hypothesis import Hypothesis
from src.core.db.models.idempotency_key import IdempotencyKey
from src.core.db.models.incident import Incident
from src.core.db.models.incident_change import IncidentChange
from src.core.db.models.incident_service import IncidentService
from src.core.db.models.ingest_receipt import IngestReceipt
from src.core.db.models.investigation import Investigation
from src.core.db.models.investigation_event import InvestigationEvent
from src.core.db.models.investigation_result import InvestigationResult
from src.core.db.models.project_config import ProjectConfig
from src.core.db.models.project_tombstone import ProjectTombstone
from src.core.db.models.quota_counter import QuotaCounter
from src.core.db.models.rca_report import RcaReport
from src.core.db.models.review import Review
from src.core.db.models.timeline_entry import TimelineEntry

inbox = inbox_table(Base.metadata)
outbox = outbox_table(Base.metadata)

__all__ = [
    "Alert",
    "AuditEvent",
    "AutostartGate",
    "Base",
    "CorrelationSlot",
    "Evidence",
    "Hypothesis",
    "IdempotencyKey",
    "Incident",
    "IncidentChange",
    "IncidentService",
    "IngestReceipt",
    "Investigation",
    "InvestigationEvent",
    "InvestigationResult",
    "ProjectConfig",
    "ProjectTombstone",
    "QuotaCounter",
    "RcaReport",
    "Review",
    "TimelineEntry",
    "inbox",
    "outbox",
]
