"""Poll Binance /futures/data/* every 5 minutes -> crypto.futures_metrics.

These aggregated stats have no WebSocket stream, so we poll REST. One row per
symbol per poll, schema-aligned with the Vision `metrics` backfill.
"""
from __future__ import annotations

import time

import requests

import ch
import config
import tables
from util import ms_to_dt, now_utc, dec

_FD = f"{config.BINANCE_FAPI}/futures/data"
MKT, EXC = config.MARKET_TYPE, config.EXCHANGE


def _latest(path: str, symbol: str) -> dict | None:
    try:
        r = requests.get(f"{_FD}/{path}",
                         params={"symbol": symbol, "period": "5m", "limit": 1}, timeout=15)
        r.raise_for_status()
        arr = r.json()
        return arr[-1] if arr else None
    except Exception:
        return None


def _row(symbol: str) -> tuple | None:
    oi = _latest("openInterestHist", symbol)
    if not oi:
        return None
    top_acc = _latest("topLongShortAccountRatio", symbol) or {}
    top_pos = _latest("topLongShortPositionRatio", symbol) or {}
    glob = _latest("globalLongShortAccountRatio", symbol) or {}
    taker = _latest("takerlongshortRatio", symbol) or {}
    ts = ms_to_dt(oi["timestamp"])
    return (
        EXC, MKT, symbol, ts,
        dec(oi.get("sumOpenInterest")), dec(oi.get("sumOpenInterestValue")),
        dec(top_acc.get("longShortRatio")), dec(top_pos.get("longShortRatio")),
        dec(glob.get("longShortRatio")), dec(taker.get("buySellRatio")),
        ts, now_utc(), {"src": "rest_poll"},
    )


def run(symbols: list[str], seconds: int = 0, interval: int = 300) -> None:
    client = ch.get_client()
    deadline = time.time() + seconds if seconds else None
    while True:
        rows = [r for r in (_row(s) for s in symbols) if r is not None]
        ch.insert(client, "futures_metrics", rows, tables.METRICS_COLS)
        print(f"[metrics] inserted {len(rows)} rows", flush=True)
        if deadline and time.time() >= deadline:
            return
        # sleep in small steps so --seconds smoke runs stop promptly
        end = time.time() + interval
        while time.time() < end:
            if deadline and time.time() >= deadline:
                return
            time.sleep(min(5, end - time.time()))
