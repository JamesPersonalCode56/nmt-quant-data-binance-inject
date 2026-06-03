"""Shared helpers: time conversion and decimal parsing."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

_EPOCH = datetime(1970, 1, 1)  # naive UTC; ClickHouse DateTime64(_,'UTC') columns store naive-UTC


def now_utc() -> datetime:
    """Naive-UTC 'now' with millisecond resolution (matches DateTime64(3)).

    `datetime.utcnow()` is deprecated in 3.12; build an aware-UTC then drop tzinfo
    so it still matches the naive-UTC DateTime64 columns.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ms_to_dt(ms: int | str) -> datetime:
    """Epoch milliseconds -> naive-UTC datetime, exact to the millisecond."""
    ms = int(ms)
    return _EPOCH + timedelta(milliseconds=ms)


def str_to_dt(s: str) -> datetime:
    """'2024-09-15 00:00:05' (UTC) -> naive datetime."""
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")


def dec(x) -> Decimal | None:
    """Exact Decimal from a string/number; None/'' -> None."""
    if x is None or x == "":
        return None
    return Decimal(str(x))
