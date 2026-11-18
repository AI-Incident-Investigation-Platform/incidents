ACTIVE = frozenset({"pending", "running"})
TERMINAL = frozenset({"completed", "inconclusive", "failed", "timed_out", "canceled"})


class InvestigationConflict(Exception):
    pass


def next_attempt(history: list[tuple[str, int]]) -> int:
    if any(status in ACTIVE for status, _ in history):
        raise InvestigationConflict("У инцидента уже идёт расследование")
    return max((attempt for _, attempt in history), default=0) + 1


def retry_allowed(status: str) -> bool:
    return status in TERMINAL
