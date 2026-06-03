"""Parse Vision CSVs (inside the downloaded .zip) into ClickHouse insert rows.

Each parser yields tuples in the column order defined in tables.py.
"""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import datetime

import config
import tables
from util import ms_to_dt, str_to_dt, dec

MKT = config.MARKET_TYPE
EXC = config.EXCHANGE


def _open_csv(zip_path: str):
    """Yield csv rows from the single CSV inside the zip, skipping a header if present."""
    with zipfile.ZipFile(zip_path) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            reader = csv.reader(io.TextIOWrapper(fh, "utf-8"))
            first = True
            for row in reader:
                if not row:
                    continue
                if first:
                    first = False
                    # header row has a non-numeric first cell (id/timestamp/create_time)
                    if not row[0].replace(".", "", 1).isdigit():
                        continue
                yield row


def parse_trades(zip_path: str, symbol: str, ingested_at: datetime) -> list[tuple]:
    # id,price,qty,quote_qty,time,is_buyer_maker
    out = []
    for r in _open_csv(zip_path):
        ts = ms_to_dt(r[4])
        out.append((
            EXC, MKT, symbol, int(r[0]), dec(r[1]), dec(r[2]), dec(r[3]),
            ts, 1 if r[5].strip().lower() == "true" else 0,
            ts, ingested_at, {"src": "vision"},
        ))
    return out


def parse_book_depth(zip_path: str, symbol: str, ingested_at: datetime) -> list[tuple]:
    # timestamp,percentage,depth,notional
    out = []
    for r in _open_csv(zip_path):
        ts = str_to_dt(r[0])
        out.append((
            EXC, MKT, symbol, ts, int(float(r[1])), dec(r[2]), dec(r[3]),
            ts, ingested_at, {"src": "vision"},
        ))
    return out


def parse_metrics(zip_path: str, symbol: str, ingested_at: datetime) -> list[tuple]:
    # create_time,symbol,sum_open_interest,sum_open_interest_value,
    # count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,
    # count_long_short_ratio,sum_taker_long_short_vol_ratio
    out = []
    for r in _open_csv(zip_path):
        ts = str_to_dt(r[0])
        out.append((
            EXC, MKT, symbol, ts, dec(r[2]), dec(r[3]), dec(r[4]), dec(r[5]),
            dec(r[6]), dec(r[7]), ts, ingested_at, {"src": "vision"},
        ))
    return out


# dataset -> (parser, target table, insert columns)
SPEC = {
    "trades":    (parse_trades,     "trades",          tables.TRADES_COLS),
    "bookDepth": (parse_book_depth, "book_depth",      tables.BOOK_DEPTH_COLS),
    "metrics":   (parse_metrics,    "futures_metrics", tables.METRICS_COLS),
}
