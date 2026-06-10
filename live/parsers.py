"""Build ClickHouse rows from live Binance WS payloads."""
from __future__ import annotations

from datetime import datetime

import config
from util import ms_to_dt, dec

MKT = config.MARKET_TYPE
EXC = config.EXCHANGE


def trade_row(data: dict, ingested: datetime) -> tuple:
    """`<symbol>@aggTrade` payload -> crypto.trades row (TRADES_COLS order).

    Default live trade source (WS, via the /market route — works direct from VN).
    trade_id is the aggregate id; extra['src']='ws_agg'. REST poll_trades is the fallback.
    """
    return (
        EXC, MKT, data["s"], int(data["a"]),
        dec(data["p"]), dec(data["q"]), None,
        ms_to_dt(data["T"]), 1 if data["m"] else 0,
        ms_to_dt(data["E"]), ingested, {"src": "ws_agg"},
    )


def l2_rows(data: dict, ingested: datetime, levels: int, stream: str = "") -> list[tuple]:
    """`<symbol>@depth<N>@100ms` payload -> crypto.book_snapshot_l2 rows (BOOK_L2_COLS).

    Futures payload has s/E/T and bids/asks under 'b'/'a'. Spot partial-depth payload is
    just {lastUpdateId, bids, asks} (no symbol/time) -> take symbol from the stream name
    and timestamp from arrival.
    """
    if MKT == "spot":
        sym = (stream.split("@", 1)[0] or data.get("s", "")).upper()
        ts = src = ingested
        bids, asks = data.get("bids", []), data.get("asks", [])
    else:
        sym = data["s"]
        ts = ms_to_dt(data.get("T") or data["E"])
        src = ms_to_dt(data["E"])
        bids, asks = data.get("b", []), data.get("a", [])
    out = []
    for side, levs in (("bid", bids), ("ask", asks)):
        for lvl, pq in enumerate(levs[:levels]):
            out.append((
                EXC, MKT, sym, ts, side, lvl, dec(pq[0]), dec(pq[1]),
                src, ingested, {},
            ))
    return out


def kline_rows(data: dict, ingested: datetime) -> list[tuple]:
    """`<symbol>@kline_<interval>` payload -> crypto.ohlcv rows (OHLCV_COLS).

    CLOSED candles only: returns [] until `k['x']` (kline closed) is true, so we record
    each final candle exactly once. Same payload shape on spot and futures.
    """
    k = data["k"]
    if not k.get("x"):
        return []
    return [(
        EXC, MKT, k["s"], k["i"], ms_to_dt(k["t"]),
        dec(k["o"]), dec(k["h"]), dec(k["l"]), dec(k["c"]),
        dec(k["v"]), dec(k["q"]), int(k["n"]), dec(k["V"]), dec(k["Q"]),
        1, ms_to_dt(data["E"]), ingested, {},
    )]


def liquidation_rows(data: dict, ingested: datetime) -> list[tuple]:
    """`!forceOrder@arr` / `<symbol>@forceOrder` payload -> crypto.liquidations rows.

    Futures-only forced-liquidation feed (delivered on the /market route, same as
    @aggTrade — the Public route returns nothing). `o['S']` is the liquidation
    order side: SELL = a LONG was force-closed, BUY = a SHORT was force-closed.
    """
    o = data["o"]
    return [(
        EXC, MKT, o["s"], o["S"].lower(), o["o"], o["f"],
        dec(o["q"]), dec(o["p"]), dec(o["ap"]), o["X"],
        dec(o["l"]), dec(o["z"]),
        ms_to_dt(o["T"]), ms_to_dt(data["E"]), ingested, {},
    )]
