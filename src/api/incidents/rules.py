from typing import Any


class IncidentConflict(Exception):
    pass


def resolve_status(current: str, target: str, *, active_investigation: bool) -> str:
    if target not in {"open", "resolved"}:
        raise ValueError("Недопустимый статус")
    if target == "resolved" and current != "resolved" and active_investigation:
        raise IncidentConflict("Активное расследование мешает закрыть инцидент")
    if target == "open" and current != "resolved" and current != "open":
        raise IncidentConflict("Повторное открытие возможно после Resolve")
    return target


def validate_patch(values: dict[str, Any]) -> dict[str, Any]:
    allowed = {"title", "description", "severity", "service_ids"}
    if not values or set(values) - allowed:
        raise ValueError("Нет допустимых полей для изменения")
    if "title" in values:
        title = values["title"]
        if not isinstance(title, str) or not title.strip() or len(title) > 300:
            raise ValueError("Заголовок должен содержать от 1 до 300 символов")
        values["title"] = title.strip()
    if "severity" in values and values["severity"] not in {"critical", "high", "medium", "low"}:
        raise ValueError("Недопустимая серьёзность")
    return values
