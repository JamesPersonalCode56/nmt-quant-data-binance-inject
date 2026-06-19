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
import time

import ch
import config
import symbols as symmod
import tables
from live import parsers, poll_metrics, poll_oi_funding, poll_trades, wsmanager
from util import now_utc

FLUSH_RETRIES = 4            # CH insert attempts before a flush is deemed unrecoverable
FLUSH_RETRY_BACKOFF = 1.0    # seconds, multiplied by attempt number (1s, 2s, 3s)

# Supervisor: re-spawn dead workers (run-forever mode only) with bounded backoff.
SUPERVISE_POLL = 3.0         # seconds between liveness sweeps
RESPAWN_BACKOFF_START = 1.0  # initial per-slot backoff after a crash
RESPAWN_BACKOFF_CAP = 30.0   # max per-slot backoff
RESPAWN_BACKOFF_RESET = 60.0 # a slot alive this long resets its backoff to START


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
        if not buf:
            return
        rows, buf = buf, []
        # Retry transient CH errors (e.g. "Unexpected Http Driver Exception" during
        # the daily archive load) before giving up. On final failure the exception
        # propagates out of flusher() and is surfaced (worker exits, supervisor restarts).
        for attempt in range(FLUSH_RETRIES):
            try:
                await loop.run_in_executor(None, ch.insert, client, table, rows, cols)
                return
            except Exception as e:
                if attempt == FLUSH_RETRIES - 1:
                    raise
                print(f"[{table} pid={os.getpid()}] insert retry {attempt + 1}/"
                      f"{FLUSH_RETRIES} after error: {e}", flush=True)
                await asyncio.sleep(FLUSH_RETRY_BACKOFF * (attempt + 1))

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
    st = asyncio.create_task(
        wsmanager.stream(streams, handle, stop, proxy=proxy, base=base))
    try:
        # Wait on the stream AND the flusher: if the flusher dies (unrecoverable CH
        # insert failure) we must NOT keep streaming into a buffer nobody drains
        # (the full.wait() high-water path would deadlock). Surface it so the worker
        # exits and the supervisor restarts it.
        done, _ = await asyncio.wait({st, ft}, return_when=asyncio.FIRST_COMPLETED)
        if ft in done and not ft.cancelled() and ft.exception() is not None:
            raise ft.exception()  # type: ignore[misc]
    finally:
        stop.set()
        ft.cancel()
        st.cancel()
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


def _chunk(xs: list, n: int) -> list[list]:
    n = max(1, min(n, len(xs)))
    return [xs[i::n] for i in range(n)]


# A worker slot: how to (re)build its process, plus a human label for logging.
WorkerSpec = tuple  # (target: Callable, args: tuple, label: str)


def _spawn(spec: WorkerSpec) -> mp.Process:
    target, args, _label = spec
    p = mp.Process(target=target, args=args)
    p.start()
    return p


def _supervise(
    specs: list[WorkerSpec],
    spawn=_spawn,
    poll: float = SUPERVISE_POLL,
    stop_after=None,
    sleep=time.sleep,
    now=time.monotonic,
) -> None:
    """Run-forever supervisor: start every slot, re-spawn any that die.

    Each slot gets bounded per-slot backoff so a worker that crashes instantly on a
    persistent error doesn't hot-loop; a slot that stays alive `RESPAWN_BACKOFF_RESET`s
    resets its backoff. `stop_after(iteration)` (test hook) returns True to break the
    loop; in production it is None (loop forever). `spawn`/`sleep`/`now` are injectable
    so the loop is unit-testable without real processes or wall-clock waits.
    """
    procs = [spawn(s) for s in specs]
    backoff = [RESPAWN_BACKOFF_START] * len(specs)
    started = [now()] * len(specs)
    next_try = [0.0] * len(specs)  # earliest monotonic time we may re-spawn a slot
    try:
        it = 0
        while True:
            if stop_after is not None and stop_after(it):
                break
            t = now()
            for i, p in enumerate(procs):
                if p.is_alive():
                    if t - started[i] >= RESPAWN_BACKOFF_RESET:
                        backoff[i] = RESPAWN_BACKOFF_START
                    continue
                if t < next_try[i]:
                    continue  # still backing off this slot
                _target, _args, label = specs[i]
                print(f"[supervisor] worker '{label}' (pid={p.pid}) died "
                      f"exitcode={p.exitcode}; re-spawning (backoff={backoff[i]:.0f}s)",
                      flush=True)
                procs[i] = spawn(specs[i])
                started[i] = now()
                next_try[i] = started[i] + backoff[i]
                backoff[i] = min(backoff[i] * 2, RESPAWN_BACKOFF_CAP)
            it += 1
            sleep(poll)
    except KeyboardInterrupt:
        print("\nstopping...", flush=True)
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()


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
    ap.add_argument("--rest-trades", action="store_true",
                    help="collect trades via REST pagination instead of WS @aggTrade")
    args = ap.parse_args()

    syms = symmod.resolve(args.symbols)
    os.environ["CH_HOST"] = config.resolve_ch_host()   # forked children skip re-probing
    is_spot = config._SPOT
    ws_trades = config.WS_TRADES and not args.rest_trades and not args.no_trades
    run_metrics = not args.no_metrics and not is_spot          # long/short ratios: futures only
    run_oi_funding = not args.no_oi_funding and not is_spot    # OI + funding: futures only
    ob_groups = [g for g in _chunk(syms, args.groups) if g]
    kl_groups = [g for g in _chunk(syms, args.kline_groups) if g]
    print(f"market={config.MARKET_TYPE}  host={os.environ['CH_HOST']}  symbols={len(syms)}  "
          f"ob_groups={len(ob_groups)}  kline_groups={len(kl_groups) if not args.no_klines else 0}"
          f"x{len(config.KLINE_INTERVALS)}iv  "
          f"trades={'ws' if ws_trades else ('rest' if not args.no_trades else 'off')}  "
          f"oi_funding={run_oi_funding}  metrics={run_metrics}  "
          f"seconds={args.seconds or 'forever'}", flush=True)

    # One spec per worker slot: (target, args, label). The supervisor re-spawns a
    # slot from its spec when its process dies; smoke-test mode just starts+joins.
    secs = args.seconds
    specs: list[WorkerSpec] = []
    if not args.no_orderbook:                          # depth -> Public route
        specs += [(_run_orderbook, (g, secs), f"orderbook-{i}") for i, g in enumerate(ob_groups)]
    if not args.no_klines:                             # @kline_* -> ohlcv (closed only), /market
        specs += [(_run_klines, (g, secs), f"klines-{i}") for i, g in enumerate(kl_groups)]
    if not args.no_trades:
        if ws_trades:                                  # @aggTrade via /market route (default)
            specs.append((_run_ws_trades, (syms, secs), "ws-trades"))
        else:                                          # REST paginator fallback (--rest-trades)
            specs.append((poll_trades.run, (syms, secs), "rest-trades"))
    if run_oi_funding:                                 # real-time open_interest + funding_rate
        specs.append((poll_oi_funding.run, (syms, secs), "oi-funding"))
    if run_metrics:
        specs.append((poll_metrics.run, (syms, secs), "metrics"))

    if secs > 0:
        # Smoke test: workers are SUPPOSED to exit at the deadline -> no supervision,
        # keep the original start+join+exit behavior so a clean exit isn't a "crash".
        procs = [_spawn(s) for s in specs]
        try:
            for p in procs:
                p.join()
        except KeyboardInterrupt:
            print("\nstopping...", flush=True)
            for p in procs:
                p.terminate()
            for p in procs:
                p.join()
    else:
        _supervise(specs)                              # run forever: re-spawn dead workers


if __name__ == "__main__":
    main()
