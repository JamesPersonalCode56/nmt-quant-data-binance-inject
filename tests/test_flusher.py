"""A failing flusher surfaces (not swallowed): _ws_loop re-raises so the worker exits."""

from __future__ import annotations

import asyncio

import pytest

import ch
from live import main


def test_flusher_failure_propagates(monkeypatch):
    """When ch.insert fails on every retry, _ws_loop raises instead of deadlocking."""
    monkeypatch.setattr(ch, "get_client", lambda: object())
    # Make retries instant.
    monkeypatch.setattr(main, "FLUSH_RETRIES", 2)
    monkeypatch.setattr(main, "FLUSH_RETRY_BACKOFF", 0.0)

    def boom(_client, _table, _rows, _cols):
        raise RuntimeError("Unexpected Http Driver Exception")

    monkeypatch.setattr(ch, "insert", boom)

    async def fake_stream(streams, handle, stop, proxy=None, base=""):
        # Feed one message so the buffer is non-empty, then idle until cancelled.
        handle('{"data": {"x": 1}, "stream": "s"}', 1)
        await asyncio.Event().wait()

    monkeypatch.setattr(main.wsmanager, "stream", fake_stream)

    async def run():
        await main._ws_loop(
            symbols=["BTCUSDT"],
            seconds=0,
            streams_for=lambda s: ["s"],
            table="trades",
            cols=["x"],
            build=lambda d, st: [(d["x"],)],
            proxy=None,
            base="",
        )

    with pytest.raises(RuntimeError, match="Http Driver Exception"):
        asyncio.run(asyncio.wait_for(run(), timeout=5))
