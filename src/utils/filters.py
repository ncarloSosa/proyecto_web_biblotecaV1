"""Custom Jinja filters."""
from __future__ import annotations

from datetime import date, datetime
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
