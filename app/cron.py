from __future__ import annotations

from datetime import datetime, timedelta


def _parse_field(field: str, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if part == "*":
            values.update(range(minimum, maximum + 1))
            continue
        if part.startswith("*/"):
            step = int(part[2:])
            values.update(range(minimum, maximum + 1, step))
            continue
        if "-" in part:
            start_s, end_s = part.split("-", 1)
            if "/" in end_s:
                end_s, step_s = end_s.split("/", 1)
                step = int(step_s)
            else:
                step = 1
            start, end = int(start_s), int(end_s)
            values.update(range(start, end + 1, step))
            continue
        values.add(int(part))
    return {v for v in values if minimum <= v <= maximum}


def cron_matches(expr: str, dt: datetime) -> bool:
    fields = expr.split()
    if len(fields) != 5:
        raise ValueError("cron expressions must have 5 fields: m h dom mon dow")
    minute, hour, day, month, weekday = fields
    minutes = _parse_field(minute, 0, 59)
    hours = _parse_field(hour, 0, 23)
    days = _parse_field(day, 1, 31)
    months = _parse_field(month, 1, 12)
    weekdays = _parse_field(weekday, 0, 6)
    # Sunday as 0 or 7
    if 7 in weekdays:
        weekdays.add(0)
    return (
        dt.minute in minutes
        and dt.hour in hours
        and dt.day in days
        and dt.month in months
        and dt.weekday() % 7 in weekdays
    )


def next_cron(expr: str, after: datetime, limit_minutes: int = 366 * 24 * 60) -> datetime:
    cursor = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(limit_minutes):
        if cron_matches(expr, cursor):
            return cursor
        cursor += timedelta(minutes=1)
    raise ValueError(f"no match for cron {expr!r}")
