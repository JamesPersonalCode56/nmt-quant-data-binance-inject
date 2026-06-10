"""Live collection -> ClickHouse.

Worker processes (sized for an 8-CPU box shared with ClickHouse). The venue is fixed by
MARKET_TYPE: `um` (USDM futures, default) or `spot`.
  * order book : N groups, each a combined `@depth<N>@100ms` WebSocket on the Public
                 route, via the gap-free wsmanager (Binance's 24h rotation). -> book_snapshot_l2
  * trades     : ONE `@aggTrade` WebSocket on the /market route (futures; works direct
                 from VN — no VPN). `--rest-trades` switches to a REST paginator. -> trades
  * klines     : K groups of `@kline_<interval>` (ALL intervals) on the /market route,
                 inserting CLOSED candles only. -> ohlcv
  * oi+funding : ONE REST poller (futures only) -> open_interest + funding_rate (real-time)
  * metrics    : ONE REST poller (futures only) -> futures_metrics (long/short ratios, 5m)

Usage:
  python -m live.main                                   # pairs from pairs.yaml, run forever
  python -m live.main --symbols BTCUSDT,ETHUSDT --seconds 60   # smoke test
  python -m live.main --groups 3 --no-trades --no-klines
  MARKET_TYPE=spot python -m live.main --symbols PAXGUSDT,XAUTUSDT   # spot instance
"""
from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os

import ch
import config
import symbols as symmod
import tables
from live import parsers, poll_metrics, poll_oi_funding, poll_trades, wsmanager
from util import now_utc


async def _ws_loop(symbols, seconds, streams_for, table, cols, build, proxy, base) -> None:
    """Generic buffered WS collector. `streams_for(sym)`->stream names; `build(data, stream)`->rows."""
    client = ch.get_client()
    loop = asyncio.get_event_loop()
    buf: list = []
    stop = asyncio.Event()
    full = asyncio.Event()        # set when buf reaches the row high-water mark
    if seconds:
        loop.call_later(seconds, stop.set)

    def handle(msg: str, gen: int) -> None:
        # runs in this same event-loop thread (wsmanager.recv loop) -> Event.set() is safe
        obj = json.loads(msg)
        data = obj.get("data")
        if data:
            buf.extend(build(data, obj.get("stream", "")))
            if len(buf) >= config.LIVE_FLUSH_ROWS:
                full.set()

    async def flush() -> None:
        nonlocal buf
        if buf:
            rows, buf = buf, []
            await loop.run_in_executor(None, ch.insert, client, table, rows, cols)

    async def flusher() -> None:
        # flush on whichever comes first: LIVE_FLUSH_SECS elapsed, or buf hits
        # LIVE_FLUSH_ROWS. The time path is the normal one (keeps CH part-count low);
        # the row path only fires under bursts/backpressure to bound memory.
        while not stop.is_set():
            try:
                await asyncio.wait_for(full.wait(), timeout=config.LIVE_FLUSH_SECS)
            except asyncio.TimeoutError:
                pass
            full.clear()
            await flush()

    streams = [s for sym in symbols for s in streams_for(sym)]
    ft = asyncio.create_task(flusher())
    try:
        await wsmanager.stream(streams, handle, stop, proxy=proxy, base=base)
    finally:
        stop.set()
        ft.cancel()
        await flush()


def _run_orderbook(symbols: list[str], seconds: int) -> None:
    """Order book depth -> book_snapshot_l2 (Public route)."""
    levels = config.L2_LEVELS
    print(f"[orderbook pid={os.getpid()}] {len(symbols)} symbols", flush=True)
    asyncio.run(_ws_loop(
        symbols, seconds,
        streams_for=lambda s: [f"{s.lower()}@depth{levels}@100ms"],
        table="book_snapshot_l2", cols=tables.BOOK_L2_COLS,
        build=lambda d, st: parsers.l2_rows(d, now_utc(), levels, st),
        proxy=None, base=config.WS_PUBLIC_BASE))


def _run_ws_trades(symbols: list[str], seconds: int) -> None:
    """Trades via WS @aggTrade -> trades (futures: /market route, works direct from VN)."""
    print(f"[ws-trades pid={os.getpid()}] {len(symbols)} symbols  proxy={config.WS_PROXY}", flush=True)
    asyncio.run(_ws_loop(
        symbols, seconds,
        streams_for=lambda s: [f"{s.lower()}@aggTrade"],
        table="trades", cols=tables.TRADES_COLS,
        build=lambda d, st: [parsers.trade_row(d, now_utc())],
        proxy=config.WS_PROXY, base=config.WS_MARKET_BASE))


def _run_klines(symbols: list[str], seconds: int) -> None:
    """All-interval `@kline_<iv>` -> ohlcv, CLOSED candles only (price stream: /market route)."""
    ivs = config.KLINE_INTERVALS
    print(f"[klines pid={os.getpid()}] {len(symbols)} symbols x {len(ivs)} intervals", flush=True)
    asyncio.run(_ws_loop(
        symbols, seconds,
        streams_for=lambda s: [f"{s.lower()}@kline_{iv}" for iv in ivs],
        table="ohlcv", cols=tables.OHLCV_COLS,
        build=lambda d, st: parsers.kline_rows(d, now_utc()),
        proxy=config.WS_PROXY, base=config.WS_MARKET_BASE))


def _run_liquidations(seconds: int) -> None:
    """All-market forced liquidations `!forceOrder@arr` -> crypto.liquidations (futures only).

    One connection on the /market route covers the WHOLE futures market (cheap,
    ~1-2 events/s), so it ignores the per-symbol universe on purpose — cross-symbol
    liquidation cascades are themselves the signal. The Public route returns nothing
    for this stream.
    """
    print(f"[liquidations pid={os.getpid()}] !forceOrder@arr (all-market)", flush=True)
    asyncio.run(_ws_loop(
        ["_"], seconds,                              # one dummy symbol -> a single stream
        streams_for=lambda _s: ["!forceOrder@arr"],
        table="liquidations", cols=tables.LIQUIDATIONS_COLS,
        build=lambda d, st: parsers.liquidation_rows(d, now_utc()),
        proxy=config.WS_PROXY, base=config.WS_MARKET_BASE))


def _chunk(xs: list, n: int) -> list[list]:
    n = max(1, min(n, len(xs)))
    return [xs[i::n] for i in range(n)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=None, help="csv list, or demo|core|all")
    ap.add_argument("--seconds", type=int, default=0, help="0 = run forever")
    ap.add_argument("--groups", type=int, default=config.LIVE_GROUPS, help="order-book WS processes")
    ap.add_argument("--kline-groups", type=int, default=config.KLINE_GROUPS, help="kline WS processes")
    ap.add_argument("--no-trades", action="store_true")
    ap.add_argument("--no-klines", action="store_true")
    ap.add_argument("--no-metrics", action="store_true")
    ap.add_argument("--no-oi-funding", action="store_true")
    ap.add_argument("--no-orderbook", action="store_true")
    ap.add_argument("--no-liquidations", action="store_true")
    ap.add_argument("--rest-trades", action="store_true",
                    help="collect trades via REST pagination instead of WS @aggTrade")
    args = ap.parse_args()

    syms = symmod.resolve(args.symbols)
    os.environ["CH_HOST"] = config.resolve_ch_host()   # forked children skip re-probing
    is_spot = config._SPOT
    ws_trades = config.WS_TRADES and not args.rest_trades and not args.no_trades
    run_metrics = not args.no_metrics and not is_spot          # long/short ratios: futures only
    run_oi_funding = not args.no_oi_funding and not is_spot    # OI + funding: futures only
    run_liquidations = not args.no_liquidations and not is_spot  # forced liquidations: futures only
    ob_groups = [g for g in _chunk(syms, args.groups) if g]
    kl_groups = [g for g in _chunk(syms, args.kline_groups) if g]
    print(f"market={config.MARKET_TYPE}  host={os.environ['CH_HOST']}  symbols={len(syms)}  "
          f"ob_groups={len(ob_groups)}  kline_groups={len(kl_groups) if not args.no_klines else 0}"
          f"x{len(config.KLINE_INTERVALS)}iv  "
          f"trades={'ws' if ws_trades else ('rest' if not args.no_trades else 'off')}  "
          f"oi_funding={run_oi_funding}  metrics={run_metrics}  "
          f"liquidations={run_liquidations}  "
          f"seconds={args.seconds or 'forever'}", flush=True)

    procs: list[mp.Process] = []
    if not args.no_orderbook:                          # depth -> Public route
        procs += [mp.Process(target=_run_orderbook, args=(g, args.seconds)) for g in ob_groups]
    if not args.no_klines:                             # @kline_* -> ohlcv (closed only), /market
        procs += [mp.Process(target=_run_klines, args=(g, args.seconds)) for g in kl_groups]
    if not args.no_trades:
        if ws_trades:                                  # @aggTrade via /market route (default)
            procs.append(mp.Process(target=_run_ws_trades, args=(syms, args.seconds)))
        else:                                          # REST paginator fallback (--rest-trades)
            procs.append(mp.Process(target=poll_trades.run, args=(syms, args.seconds)))
    if run_oi_funding:                                 # real-time open_interest + funding_rate
        procs.append(mp.Process(target=poll_oi_funding.run, args=(syms, args.seconds)))
    if run_metrics:
        procs.append(mp.Process(target=poll_metrics.run, args=(syms, args.seconds)))
    if run_liquidations:                               # all-market !forceOrder@arr, /market route
        procs.append(mp.Process(target=_run_liquidations, args=(args.seconds,)))

    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("\nstopping...", flush=True)
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()


if __name__ == "__main__":
    main()
