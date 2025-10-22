"""Formatting helpers for UI presentation."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytz

from .constants import CSV_DATETIME_FORMAT


def format_currency(value: Optional[float]) -> str:
    """Return a human friendly currency string."""
    if value is None:
        return "—"
    return f"${value:,.2f}"


def format_datetime(value: Optional[datetime], tz: Optional[str] = None) -> str:
    """Render datetime with timezone awareness."""
    if not value:
        return "—"
    target = value
    if tz:
        try:
            target = value.astimezone(pytz.timezone(tz))
        except Exception:
            pass
    return target.strftime(CSV_DATETIME_FORMAT)
