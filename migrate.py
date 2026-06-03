"""Create the new crypto.* tables if they do not exist. Never alters existing tables.

Usage:  python migrate.py             # create missing tables, show result
        python migrate.py --dry       # print DDL only
        python migrate.py --recreate  # DROP & recreate the 5 NEW tables (applies CODECs).
                                       # The 4 pre-existing tables are NEVER dropped.
"""
from __future__ import annotations

import sys

import ch
import config
from tables import DDL

EXISTING_KEEP = {"symbol_info", "ohlcv", "funding_rate", "open_interest"}


def main() -> None:
    dry = "--dry" in sys.argv
    recreate = "--recreate" in sys.argv
    if dry:
        for name, ddl in DDL.items():
            print(f"-- {name}\n{ddl.strip()}\n")
        return

    client = ch.get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.CH_DATABASE}")

    before = set(_tables(client))
    for name, ddl in DDL.items():
        assert name not in EXISTING_KEEP, f"refusing to manage pre-existing table {name}"
        if recreate:
            client.command(f"DROP TABLE IF EXISTS {config.CH_DATABASE}.{name}")
            print(f"  dropped crypto.{name}")
        client.command(ddl)
        print(f"  ensured crypto.{name}")

    after = _tables(client)
    print("\nTables in crypto now:")
    for t in sorted(after):
        tag = " (pre-existing, untouched)" if t in EXISTING_KEEP else ""
        created = " [created this run]" if t not in before and t not in EXISTING_KEEP else ""
        print(f"  - {t}{tag}{created}")

    missing = EXISTING_KEEP - set(after)
    if missing:
        print(f"\nWARNING: expected pre-existing tables missing: {missing}")


def _tables(client) -> list[str]:
    return [r[0] for r in client.query(
        f"SHOW TABLES FROM {config.CH_DATABASE}").result_rows]


if __name__ == "__main__":
    main()
