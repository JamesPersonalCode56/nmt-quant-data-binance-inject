"""Gap-free Binance WebSocket connection manager.

Binance closes every WS connection at the 24h mark. To avoid losing data across
that boundary we use **make-before-break** rotation: a few seconds before a
connection reaches its self-imposed end-of-life, we open the replacement and let
both run in parallel for `OVERLAP` seconds, then drop the old one. Events that
arrive on both connections during the overlap are harmless — the destination
tables are ReplacingMergeTree keyed so the duplicate collapses on merge.

Also handles: unexpected disconnects (immediate reconnect w/ backoff) and silent
connections (no data for `STALE`s -> reconnect). `ping_interval=None` so we never
send client pings Binance won't answer; the `websockets` lib auto-pongs Binance's
server pings.
"""
from __future__ import annotations

import asyncio

import websockets

import config

ROTATE_AFTER = 23 * 3600   # rotate well before Binance's 24h hard close
OVERLAP = 15.0             # seconds the replacement runs alongside the old connection
RECV_TICK = 5.0            # recv() timeout granularity (lets us check age/staleness)
STALE = 30.0               # no message for this long -> treat connection as dead


def _url(streams: list[str], base: str) -> str:
    return f"{base}?streams=" + "/".join(streams)


async def _run_one(streams, handle, stop: asyncio.Event, eol: asyncio.Event, gen: int,
                   proxy: str | None, base: str) -> str:
    """One physical connection. Returns when it reaches end-of-life / stops / goes stale.

    Sets `eol` ~OVERLAP seconds before its end-of-life so the supervisor can open a
    replacement while this one is still delivering data. `proxy` routes only THIS
    connection (None = direct).
    """
    loop = asyncio.get_event_loop()
    start = last = loop.time()
    async with websockets.connect(_url(streams, base), ping_interval=None, max_queue=None,
                                  open_timeout=20, close_timeout=5,
                                  proxy=proxy) as ws:  # None = direct
        while not stop.is_set():
            age = loop.time() - start
            if age >= ROTATE_AFTER:
                return "rotate"
            if age >= ROTATE_AFTER - OVERLAP and not eol.is_set():
                eol.set()
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=RECV_TICK)
                last = loop.time()
                handle(msg, gen)
            except asyncio.TimeoutError:
                if loop.time() - last > STALE:
                    return "stale"
        return "stop"


async def _reap(task: asyncio.Task) -> None:
    try:
        await task
    except Exception:
        pass


async def stream(streams: list[str], handle, stop: asyncio.Event,
                 proxy: str | None = None, base: str = config.WS_PUBLIC_BASE) -> None:
    """Keep `streams` flowing into `handle(msg, gen)` until `stop` is set.

    `base` selects the route (WS_PUBLIC_BASE for depth/bookTicker, WS_MARKET_BASE for
    aggTrade/markPrice/kline). `proxy` routes only these connections (usually None).
    """
    backoff = 1.0
    gen = 0
    while not stop.is_set():
        gen += 1
        eol = asyncio.Event()
        conn = asyncio.create_task(_run_one(streams, handle, stop, eol, gen, proxy, base))
        eol_wait = asyncio.create_task(eol.wait())
        done, _ = await asyncio.wait({conn, eol_wait}, return_when=asyncio.FIRST_COMPLETED)

        if conn in done:
            eol_wait.cancel()
            if conn.exception() is not None:           # errored -> backoff then reconnect
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 1.0                              # clean rotate/stale/stop -> reopen now
            continue

        # end-of-life reached while still connected: open the replacement NOW (overlap),
        # let the old connection finish its remaining OVERLAP window in the background.
        backoff = 1.0
        asyncio.create_task(_reap(conn))
        # loop -> a fresh connection opens immediately, giving make-before-break overlap
