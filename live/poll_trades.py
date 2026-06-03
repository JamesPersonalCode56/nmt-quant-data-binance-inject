"""Live trades via REST pagination (/fapi/v1/aggTrades) — the `--rest-trades` fallback.

The default live trade source is the @aggTrade WebSocket on the /market route (see
live/main.py); this REST path is the fallback for when WS is unavailable. We tail each
symbol by aggregate-trade id: every cycle we pull all new trades since the last id we
stored (paging 1000 at a time), so no trade is missed — only slightly delayed. A
weight-aware throttle keeps us under the futures IP weight limit (read from the
X-MBX-USED-WEIGHT-1M response header).

trade_id here is the *aggregate* id (extra['src']='rest_agg'); Vision backfill uses
the full per-trade id (extra['src']='vision'). Both land in crypto.trades.
"""
from __future__ import annotations

import time

import requests

import ch
import config
import tables
from util import ms_to_dt, now_utc, dec

_AGG = config.AGGTRADES_URL          # futures /fapi/v1 or spot /api/v3 by MARKET_TYPE
MKT, EXC = config.MARKET_TYPE, config.EXCHANGE

WEIGHT_LIMIT = config.REST_WEIGHT_LIMIT   # IP weight budget per minute (2400 futures / 1200 spot)
WEIGHT_SAFETY = 0.85         # back off above this fraction
PAGE = 1000                  # max trades per request
MAX_PAGES_PER_CYCLE = 8      # cap catch-up per symbol so others aren't starved


def _get(sess: requests.Session, params: dict) -> tuple[list, int]:
    r = sess.get(_AGG, params=params, timeout=15)
    r.raise_for_status()
    used = int(r.headers.get("x-mbx-used-weight-1m", "0") or 0)
    return r.json(), used


def _throttle(used: int) -> None:
    if used >= WEIGHT_LIMIT * WEIGHT_SAFETY:
        time.sleep(5.0)


def _rows(symbol: str, trades: list) -> list[tuple]:
    out = []
    for t in trades:
        p, q = dec(t["p"]), dec(t["q"])
        ts = ms_to_dt(t["T"])
        out.append((
            EXC, MKT, symbol, int(t["a"]), p, q, (p * q if p and q else None),
            ts, 1 if t["m"] else 0, ts, now_utc(), {"src": "rest_agg"},
        ))
    return out


def _latest_id(sess: requests.Session, symbol: str) -> int:
    trades, used = _get(sess, {"symbol": symbol, "limit": 1})
    _throttle(used)
    return int(trades[-1]["a"]) if trades else 0


def _drain(sess, client, symbol: str, last_id: dict) -> int:
    """Fetch all new aggTrades since last_id[symbol]; insert; return rows written."""
    total = 0
    for _ in range(MAX_PAGES_PER_CYCLE):
        trades, used = _get(sess, {"symbol": symbol, "fromId": last_id[symbol] + 1, "limit": PAGE})
        if not trades:
            break
        rows = _rows(symbol, trades)
        ch.insert(client, "trades", rows, tables.TRADES_COLS)
        total += len(rows)
        last_id[symbol] = int(trades[-1]["a"])
        _throttle(used)
        if len(trades) < PAGE:        # caught up
            break
    return total


def run(symbols: list[str], seconds: int = 0, poll_interval: float = 3.0) -> None:
    client = ch.get_client()
    sess = requests.Session()
    last_id = {s: _latest_id(sess, s) for s in symbols}   # tail from "now"; past = backfill
    deadline = time.time() + seconds if seconds else None
    while True:
        wrote = 0
        for s in symbols:
            try:
                wrote += _drain(sess, client, s, last_id)
            except Exception as e:  # noqa: BLE001
                print(f"[trades] {s} error: {e}", flush=True)
            if deadline and time.time() >= deadline:
                print(f"[trades] inserted {wrote} rows (final)", flush=True)
                return
        print(f"[trades] inserted {wrote} rows", flush=True)
        if deadline and time.time() >= deadline:
            return
        time.sleep(poll_interval)
