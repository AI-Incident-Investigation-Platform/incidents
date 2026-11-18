import pytest

from src.api.investigations.rules import InvestigationConflict, next_attempt, retry_allowed


@pytest.mark.case(id="INC-INV-001")
def test_second_start_is_rejected_while_run_active() -> None:
    with pytest.raises(InvestigationConflict):
        next_attempt([("pending", 1)])


@pytest.mark.case(id="INC-INV-002")
def test_retry_uses_new_attempt_after_terminal() -> None:
    assert next_attempt([("failed", 1), ("canceled", 2)]) == 3
    assert retry_allowed("failed")
    assert not retry_allowed("running")
