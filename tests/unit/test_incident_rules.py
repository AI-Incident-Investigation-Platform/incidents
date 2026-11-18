import pytest

from src.api.incidents.rules import IncidentConflict, resolve_status, validate_patch


@pytest.mark.case(id="INC-MAN-001")
def test_resolve_requires_no_active_investigation() -> None:
    with pytest.raises(IncidentConflict):
        resolve_status("open", "resolved", active_investigation=True)


@pytest.mark.case(id="INC-MAN-002")
def test_resolved_incident_can_reopen() -> None:
    assert resolve_status("resolved", "open", active_investigation=False) == "open"


@pytest.mark.case(id="INC-MAN-003")
def test_resolve_is_idempotent() -> None:
    assert resolve_status("resolved", "resolved", active_investigation=False) == "resolved"


@pytest.mark.case(id="INC-MAN-004")
def test_patch_rejects_empty_title() -> None:
    with pytest.raises(ValueError):
        validate_patch({"title": "  "})


@pytest.mark.case(id="INC-MAN-005")
def test_patch_allows_clearing_description() -> None:
    assert validate_patch({"description": None}) == {"description": None}
