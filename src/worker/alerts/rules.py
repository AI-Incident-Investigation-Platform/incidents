import hashlib
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class AlertData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: UUID
    environment_id: UUID
    source: str = Field(min_length=1, max_length=200)
    source_event_id: str = Field(min_length=1, max_length=500)
    title: str = Field(min_length=1, max_length=300)
    severity: Literal["critical", "high", "medium", "low"]
    started_at: AwareDatetime
    labels: dict[str, str] = Field(default_factory=dict)
    service_ids: list[UUID] = Field(default_factory=list)
    description: str | None = None


SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}


def select_rule(alert: AlertData, rules: list[dict]) -> dict | None:
    for rule in sorted(rules, key=lambda item: int(item.get("priority", 0)), reverse=True):
        environment_id = rule.get("environment_id")
        if environment_id is not None and str(alert.environment_id) != str(environment_id):
            continue
        match = rule.get("match", {})
        labels = match.get("labels", {})
        if not all(alert.labels.get(key) == value for key, value in labels.items()):
            continue
        minimum = match.get("severity_at_least")
        if minimum is not None and SEVERITY_ORDER[alert.severity] < SEVERITY_ORDER[minimum]:
            continue
        return rule
    return None


def should_autostart(alert: AlertData, config: dict) -> bool:
    if not config.get("enabled", False):
        return False
    minimum = config.get("min_severity", "critical")
    return SEVERITY_ORDER[alert.severity] >= SEVERITY_ORDER[minimum]


def correlation_key(alert: AlertData, keys: list[str] | None = None) -> str:
    labels = (
        alert.labels if keys is None else {key: alert.labels.get(key) for key in sorted(set(keys))}
    )
    payload = {
        "project_id": str(alert.project_id),
        "environment_id": str(alert.environment_id),
        "source": alert.source,
        "labels": labels,
        "service_ids": sorted(str(value) for value in alert.service_ids),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def payload_hash(alert: AlertData) -> str:
    canonical = json.dumps(alert.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def same_episode(episode_until: datetime, now: datetime) -> bool:
    return now <= episode_until


def parse_alert(project_id: UUID, data: dict) -> AlertData:
    payload = data["payload"]
    environment_id = UUID(data["environment_id"])
    if data["format"] == "aiip":
        alert = AlertData.model_validate(payload)
        if alert.project_id != project_id or alert.environment_id != environment_id:
            raise ValueError("Проект или окружение не соответствуют сообщению")
        return alert
    if data["format"] == "alertmanager":
        labels = payload["labels"]
        annotations = payload.get("annotations", {})
        severity = labels.get("severity", "medium")
        if severity not in {"critical", "high", "medium", "low"}:
            severity = "medium"
        return AlertData(
            project_id=project_id,
            environment_id=environment_id,
            source="alertmanager",
            source_event_id=f"{payload['fingerprint']}:{payload['startsAt']}:{payload['status']}",
            title=annotations.get("summary") or labels.get("alertname") or "Alertmanager alert",
            severity=severity,
            started_at=payload["startsAt"],
            labels=labels,
            service_ids=[],
            description=annotations.get("description"),
        )
    raise ValueError("Неизвестный формат алерта")
