"""Resolve the symbol universe to crawl, from ``pairs.yaml``.

Which pairs each instance collects is declared in ``pairs.yaml`` (one section per
MARKET_TYPE). For the active market this module expands its ``scope`` into the
concrete, venue-valid symbol list:

    scope = <named universe>  -> ``universes.<name>`` (inline list, or an external
                                 json via ``{file: ...}``)
    scope = "all"             -> every TRADING USDT pair on the venue
    scope = [SYM, ...] / csv  -> an explicit list (csv form comes from --symbols)

then appends ``extra`` and keeps only symbols tradable on the venue (futures also
maps the ``1000x`` perp names, e.g. PEPEUSDT -> 1000PEPEUSDT).
"""
from __future__ import annotations

import functools
import json
import os

import requests
import yaml

import config


def _pairs_path() -> str:
    """Location of pairs.yaml (override with PAIRS_CONFIG; default: next to this file)."""
    return os.getenv("PAIRS_CONFIG") or os.path.join(os.path.dirname(__file__), "pairs.yaml")


@functools.lru_cache(maxsize=1)
def _load_pairs() -> dict:
    with open(_pairs_path()) as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data.get("markets"), dict):
        raise ValueError(f"{_pairs_path()}: missing or invalid 'markets' section")
    return data


def _market_config(market: str) -> dict:
    markets = _load_pairs()["markets"]
    if market not in markets:
        raise KeyError(
            f"pairs.yaml has no section for MARKET_TYPE={market!r} (have: {sorted(markets)})"
        )
    return markets[market] or {}


def _demo_file_path(rel: str) -> str:
    """Resolve a universe's external json. DEMO_SYMBOLS_JSON overrides the path
    (container mounts); otherwise it is taken relative to pairs.yaml."""
    env = os.getenv("DEMO_SYMBOLS_JSON")
    if env:
        return env
    return os.path.abspath(os.path.join(os.path.dirname(_pairs_path()), rel))


def _named_universe(name: str, spec) -> list[str]:
    if isinstance(spec, list):                       # inline list (e.g. core)
        return [str(s).strip().upper() for s in spec if str(s).strip()]
    if isinstance(spec, dict) and "file" in spec:    # external json (e.g. demo)
        with open(_demo_file_path(spec["file"])) as f:
            return [s["symbol"] for s in json.load(f)["symbols"]]
    raise ValueError(f"universe '{name}' must be a list or a {{file: <path>}} mapping")


def _expand_scope(scope) -> list[str]:
    """A scope -> a raw symbol list (before venue mapping). 'all' is handled by resolve()."""
    if isinstance(scope, (list, tuple)):             # explicit list from yaml
        return [str(s).strip().upper() for s in scope if str(s).strip()]
    scope = str(scope).strip()
    universes = _load_pairs().get("universes", {})
    if scope in universes:
        return _named_universe(scope, universes[scope])
    return [s.strip().upper() for s in scope.split(",") if s.strip()]  # csv (e.g. --symbols)


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


def resolve(scope: str | list | None = None) -> list[str]:
    """Resolve the crawl list for the active MARKET_TYPE.

    `scope` defaults to ``pairs.yaml`` -> markets.<MARKET_TYPE>.scope; pass a value
    (e.g. the ``--symbols`` CLI flag) to override it for one run. The market's
    ``extra`` symbols are always appended.
    """
    mcfg = _market_config(config.MARKET_TYPE)
    valid = market_symbol_set()
    if scope is None:
        scope = mcfg.get("scope")

    if scope == "all":
        return sorted(valid)

    raw = _expand_scope(scope) + [str(s).strip().upper() for s in (mcfg.get("extra") or [])]

    out, seen = [], set()
    for s in raw:
        m = _map_to_market(s, valid)
        if m and m not in seen:
            seen.add(m)
            out.append(m)
    return out


if __name__ == "__main__":
    syms = resolve()
    print(f"market={config.MARKET_TYPE} -> {len(syms)} symbols")
    print(" ".join(syms))
