"""resolve_ch_host() retries the /ping sweep with backoff before raising."""

from __future__ import annotations

import pytest

import config


@pytest.fixture(autouse=True)
def _no_env_host_no_sleep(monkeypatch):
    """Force the candidate-probe path (no CH_HOST) and skip real backoff sleeps."""
    monkeypatch.delenv("CH_HOST", raising=False)
    monkeypatch.setattr(config.time, "sleep", lambda _s: None)
    config.resolve_ch_host.cache_clear()
    yield
    config.resolve_ch_host.cache_clear()


def test_retries_then_succeeds(monkeypatch):
    """Unreachable on the first sweeps, then a candidate answers -> returns it."""
    calls = {"n": 0}
    candidate = config._HOST_CANDIDATES[0]

    def fake_ping(host: str) -> bool:
        calls["n"] += 1
        # First two full sweeps fail; on the 3rd sweep the first candidate answers.
        return calls["n"] > 2 * len(config._HOST_CANDIDATES) and host == candidate

    monkeypatch.setattr(config, "_ping", fake_ping)
    assert config.resolve_ch_host() == candidate
    assert calls["n"] > len(config._HOST_CANDIDATES)  # proves it retried, not one-shot


def test_retries_then_raises(monkeypatch):
    """Never reachable -> raises after the bounded attempt budget."""
    sweeps = {"n": 0}

    def fake_ping(_host: str) -> bool:
        sweeps["n"] += 1
        return False

    monkeypatch.setattr(config, "_ping", fake_ping)
    with pytest.raises(RuntimeError):
        config.resolve_ch_host()
    # Every attempt sweeps the full candidate list.
    assert sweeps["n"] == config._CH_RESOLVE_ATTEMPTS * len(config._HOST_CANDIDATES)


def test_env_host_short_circuits(monkeypatch):
    """CH_HOST in env wins immediately, no probing."""
    monkeypatch.setenv("CH_HOST", "ch.example")

    def boom(_host: str) -> bool:  # pragma: no cover - must never run
        raise AssertionError("_ping called despite CH_HOST set")

    monkeypatch.setattr(config, "_ping", boom)
    config.resolve_ch_host.cache_clear()
    assert config.resolve_ch_host() == "ch.example"
