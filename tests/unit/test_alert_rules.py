from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest

from src.worker.alerts.rules import (
    AlertData,
    correlation_key,
    same_episode,
    select_rule,
    should_autostart,
)


@pytest.mark.case(id="INC-ALT-001")
def test_correlation_key_ignores_label_order() -> None:
    project_id, environment_id = uuid7(), uuid7()
    first = AlertData(
        project_id=project_id,
        environment_id=environment_id,
        source="prometheus",
        source_event_id="1",
        title="CPU",
        severity="high",
        started_at=datetime.now(UTC),
        labels={"a": "1", "b": "2"},
        service_ids=[],
    )
    second = first.model_copy(update={"labels": {"b": "2", "a": "1"}})
    assert correlation_key(first) == correlation_key(second)


@pytest.mark.case(id="INC-ALT-002")
def test_episode_expires_even_if_same_key_arrives_later() -> None:
    now = datetime.now(UTC)
    assert same_episode(now + timedelta(minutes=1), now)
    assert not same_episode(now - timedelta(seconds=1), now)


@pytest.mark.case(id="INC-ALT-006")
def test_rule_priority_and_selected_correlation_labels() -> None:
    project_id, environment_id = uuid7(), uuid7()
    alert = AlertData(
        project_id=project_id,
        environment_id=environment_id,
        source="prometheus",
        source_event_id="1",
        title="CPU",
        severity="high",
        started_at=datetime.now(UTC),
        labels={"alertname": "CPUHigh", "pod": "a"},
        service_ids=[],
    )
    rules = [
        {
            "id": str(uuid7()),
            "priority": 1,
            "match": {"labels": {}},
            "correlation": {"keys": ["pod"], "window_seconds": 60},
        },
        {
            "id": str(uuid7()),
            "priority": 9,
            "match": {"labels": {"alertname": "CPUHigh"}, "severity_at_least": "medium"},
            "correlation": {"keys": ["alertname"], "window_seconds": 900},
        },
    ]
    selected = select_rule(alert, rules)
    assert selected == rules[1]
    changed_pod = alert.model_copy(update={"labels": {"alertname": "CPUHigh", "pod": "b"}})
    assert correlation_key(alert, ["alertname"]) == correlation_key(changed_pod, ["alertname"])
    assert not should_autostart(alert, {"enabled": True, "min_severity": "critical"})
    assert should_autostart(alert, {"enabled": True, "min_severity": "medium"})
