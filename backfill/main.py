"""Historical backfill from data.binance.vision -> ClickHouse.

Multiprocessing over (dataset, symbol, date) tasks. Idempotent: skips tasks already
marked 'done' in crypto.ingest_state, and ReplacingMergeTree dedupes any overlap.

Usage:
  python -m backfill.main                                   # demo symbols, .env range/datasets
  python -m backfill.main --symbols BTCUSDT --start 2024-09-15 --end 2024-09-15
  python -m backfill.main --datasets trades,bookDepth --workers 8 --force
"""
from __future__ import annotations

import argparse
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta

import ch
import config
import symbols as symmod
from backfill import vision
from backfill.parsers import SPEC
from util import now_utc
import tables


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _worker(dataset: str, symbol: str, date_str: str) -> dict:
    """Download+parse+insert one (dataset,symbol,date). Returns a status dict."""
    parser, table, cols = SPEC[dataset]
    res = {"dataset": dataset, "symbol": symbol, "date": date_str,
           "status": "error", "rows": 0, "bytes": 0, "sha256": "", "err": ""}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            got = vision.download(dataset, symbol, date_str, tmp)
            if got is None:
                res["status"] = "empty"
                return res
            path, nbytes, sha = got
            rows = parser(path, symbol, now_utc())
            client = ch.get_client()
            ch.insert_batched(client, table, rows, cols)
            res.update(status="done", rows=len(rows), bytes=nbytes, sha256=sha)
    except Exception as e:  # noqa: BLE001 - report, don't crash the pool
        res["err"] = f"{type(e).__name__}: {e}"
    return res


def _done_keys(client) -> set[tuple[str, str, str]]:
    rows = client.query(
        "SELECT dataset, symbol, toString(date) FROM crypto.ingest_state FINAL "
        "WHERE status='done'").result_rows
    return {(r[0], r[1], r[2]) for r in rows}


def _record(client, r: dict) -> None:
    client.insert(
        "ingest_state",
        [[r["dataset"], r["symbol"], date.fromisoformat(r["date"]),
          r["status"], r["rows"], r["bytes"], r["sha256"], now_utc()]],
        column_names=tables.INGEST_STATE_COLS, database=config.CH_DATABASE)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=None, help="csv list, or demo|core|all")
    ap.add_argument("--datasets", default=",".join(config.DATASETS))
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--workers", type=int, default=config.WORKERS)
    ap.add_argument("--force", action="store_true", help="ignore ingest_state, redo all")
    args = ap.parse_args()

    syms = symmod.resolve(args.symbols)
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    if config._SPOT:                       # spot Vision only exposes `trades`
        dropped = [d for d in datasets if d != "trades"]
        if dropped:
            print(f"note: spot Vision has no {dropped} -> backfilling only 'trades'")
        datasets = [d for d in datasets if d == "trades"]
    if args.start or args.end:
        start = date.fromisoformat(args.start) if args.start else config.backfill_range()[0]
        end = date.fromisoformat(args.end) if args.end else config.backfill_range()[1]
    else:
        start, end = config.backfill_range()

    # Pin the resolved CH host so forked workers don't re-probe (avoids 127.0.0.1 timeout).
    os.environ["CH_HOST"] = config.resolve_ch_host()
    main_client = ch.get_client()

    done = set() if args.force else _done_keys(main_client)
    tasks = [(ds, s, d.isoformat())
             for ds in datasets for s in syms for d in _daterange(start, end)
             if (ds, s, d.isoformat()) not in done]

    print(f"host={os.environ['CH_HOST']}  symbols={len(syms)}  datasets={datasets}")
    print(f"range={start}..{end}  tasks={len(tasks)}  skipped(done)={len(done)}  workers={args.workers}")
    if not tasks:
        print("nothing to do.")
        return

    counts = {"done": 0, "empty": 0, "error": 0, "rows": 0}
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_worker, ds, s, d): (ds, s, d) for ds, s, d in tasks}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            _record(main_client, r)
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            counts["rows"] += r["rows"]
            flag = {"done": "✓", "empty": "·", "error": "✗"}[r["status"]]
            extra = f"  {r['err']}" if r["status"] == "error" else f"  {r['rows']} rows"
            print(f"[{i}/{len(tasks)}] {flag} {r['dataset']:>9} {r['symbol']:<14} {r['date']}{extra}")

    print(f"\nDONE  ok={counts['done']} empty={counts['empty']} error={counts['error']} "
          f"rows={counts['rows']:,}")


if __name__ == "__main__":
    main()
