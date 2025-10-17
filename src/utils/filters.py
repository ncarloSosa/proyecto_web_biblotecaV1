"""Custom Jinja filters."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any


def shortdate(value: Any) -> str:
    """Return a YYYY-MM-DD string for supported date-like values."""
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        return ""
    return s[:10]


def date10(value: Any) -> str:
    """Alias for :func:`shortdate` kept for backwards compatibility with specs."""

    return shortdate(value)


def shorttime(value: Any) -> str:
    """Return an HH:MM representation if possible."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if isinstance(value, time):
        return value.strftime("%H:%M")
    s = str(value).strip()
    if not s:
        return ""
    return s[:5]
