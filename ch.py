"""ClickHouse client factory + batched insert helper (clickhouse-connect / HTTP)."""
from __future__ import annotations

import clickhouse_connect

import config


def get_client():
    """A fresh clickhouse-connect client. Each process/worker must own its own."""
    return clickhouse_connect.get_client(
        host=config.resolve_ch_host(),
        port=config.CH_HTTP_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DATABASE,
        # large async/raw inserts; keep server-side timeouts generous
        connect_timeout=10,
        send_receive_timeout=300,
    )


def insert(client, table: str, rows: list, column_names: list[str]) -> int:
    """Insert rows (list of sequences) into crypto.<table>. Returns row count."""
    if not rows:
        return 0
    client.insert(table, rows, column_names=column_names, database=config.CH_DATABASE)
    return len(rows)


def insert_batched(client, table: str, rows: list, column_names: list[str],
                   batch: int | None = None) -> int:
    """Insert in chunks of `batch` rows to bound memory / request size."""
    batch = batch or config.INSERT_BATCH
    total = 0
    for i in range(0, len(rows), batch):
        total += insert(client, table, rows[i:i + batch], column_names)
    return total
