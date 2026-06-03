"""Resolve the symbol universe to crawl.

demo  -> HFT/demo pairs-research universe (top-50) intersected with live USDM
         perpetuals, mapping spot->futures '1000x' names (PEPEUSDT -> 1000PEPEUSDT).
core  -> the symbols hardcoded in the demo strategies.
all   -> every USDM perpetual USDT, status TRADING.
csv   -> an explicit comma-separated list (validated against USDM perps).
"""
from __future__ import annotations

import json
import os
import functools

import requests

import config

# Demo universe file. Override with DEMO_SYMBOLS_JSON (e.g. inside Docker, where the
# HFT/demo tree lives outside the build context and is mounted to a fixed path).
_DEMO_SYMBOLS_JSON = os.getenv("DEMO_SYMBOLS_JSON") or os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "demo", "pairs-research", "data", "symbols.json")
)

CORE = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "1000PEPEUSDT"]


@functools.lru_cache(maxsize=1)
def market_symbol_set() -> frozenset[str]:
    """Tradable USDT symbols for this process's MARKET_TYPE (spot or USDM perp)."""
    r = requests.get(config.EXCHANGE_INFO_URL, timeout=30)
    r.raise_for_status()
    info = r.json()
    if config._SPOT:
        return frozenset(
            s["symbol"] for s in info["symbols"]
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
        )
    return frozenset(
        s["symbol"] for s in info["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    )


def _map_to_market(sym: str, valid: frozenset[str]) -> str | None:
    """Map a symbol to its name in this market, or None if absent."""
    if sym in valid:
        return sym
    if not config._SPOT and ("1000" + sym) in valid:  # futures: PEPEUSDT -> 1000PEPEUSDT
        return "1000" + sym
    return None


def _demo_universe() -> list[str]:
    with open(_DEMO_SYMBOLS_JSON) as f:
        return [s["symbol"] for s in json.load(f)["symbols"]]


def resolve(scope: str | None = None) -> list[str]:
    scope = (scope or config.SYMBOLS_SCOPE).strip()
    valid = market_symbol_set()

    if scope == "all":
        return sorted(valid)

    if scope == "core":
        raw = CORE
    elif scope == "demo":
        raw = _demo_universe()
    else:  # explicit csv list
        raw = [s.strip().upper() for s in scope.split(",") if s.strip()]
    raw = list(raw) + config.EXTRA_SYMBOLS   # always-include extras (e.g. gold tokens)

    out, seen = [], set()
    for s in raw:
        m = _map_to_market(s, valid)
        if m and m not in seen:
            seen.add(m)
            out.append(m)
    return out


if __name__ == "__main__":
    syms = resolve()
    print(f"scope={config.SYMBOLS_SCOPE} -> {len(syms)} symbols")
    print(" ".join(syms))
