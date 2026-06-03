"""Populate crypto.symbol_info (instrument reference metadata) from Binance exchangeInfo.

One row per symbol for the universe this instance crawls (MARKET_TYPE fixes spot vs USDM
futures). symbol_info is a static-metadata table (ReplacingMergeTree keyed by
exchange,market_type,symbol -> one current row per symbol); re-running just refreshes it.

Usage:  python symbol_info.py                       # futures: demo + EXTRA_SYMBOLS
        MARKET_TYPE=spot python symbol_info.py --symbols PAXGUSDT,XAUTUSDT
"""
from __future__ import annotations

import argparse

import requests

import ch
import config
import symbols as symmod
import tables
from decimal import Decimal

from util import ms_to_dt, now_utc, dec

EXC, MKT = config.EXCHANGE, config.MARKET_TYPE


def dec0(x) -> Decimal:
    """Decimal, defaulting missing/blank to 0 (for symbol_info's NON-nullable Decimals)."""
    return dec(x) or Decimal(0)


def _decimals(s: str | None) -> int:
    """Number of significant decimal places in a step/tick string like '0.00010000'."""
    if not s or "." not in s:
        return 0
    frac = s.split(".")[1].rstrip("0")
    return len(frac)


def _filters(sym: dict) -> tuple[dict, dict, dict]:
    d = {f["filterType"]: f for f in sym.get("filters", [])}
    notional = d.get("NOTIONAL") or d.get("MIN_NOTIONAL") or {}
    return d.get("PRICE_FILTER", {}), d.get("LOT_SIZE", {}), notional


def _row(sym: dict, ingested) -> tuple:
    price, lot, notional = _filters(sym)
    tick = price.get("tickSize")
    step = lot.get("stepSize")
    is_spot = config._SPOT
    contract_type = "spot" if is_spot else ("perp" if sym.get("contractType") == "PERPETUAL" else "future")
    status = "trading" if sym.get("status") == "TRADING" else "halt"
    margin = "" if is_spot else sym.get("marginAsset", "")
    # precision: futures gives it directly; spot -> derive from tick/step
    price_prec = sym.get("pricePrecision") if not is_spot else _decimals(tick)
    qty_prec = sym.get("quantityPrecision") if not is_spot else _decimals(step)
    onboard = ms_to_dt(sym["onboardDate"]) if sym.get("onboardDate") else ms_to_dt(0)
    # min/max notional (futures MIN_NOTIONAL.notional; spot NOTIONAL.minNotional/maxNotional)
    min_notional = notional.get("notional") or notional.get("minNotional")
    max_notional = notional.get("maxNotional")
    return (
        EXC, MKT, sym["symbol"], sym.get("baseAsset", ""), sym.get("quoteAsset", ""),
        contract_type, margin, margin, 0, dec0("1"),
        int(price_prec or 0), int(qty_prec or 0),
        dec0(tick), dec0(step), dec0(lot.get("minQty")), dec0(lot.get("maxQty")),
        dec0(min_notional), dec0(max_notional),
        status, onboard, None,
        sym["symbol"].lower(), sym["symbol"], {}, ingested,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=None, help="csv list, or demo|core|all")
    args = ap.parse_args()

    wanted = set(symmod.resolve(args.symbols))
    info = requests.get(config.EXCHANGE_INFO_URL, timeout=30).json()
    ing = now_utc()
    rows = [_row(s, ing) for s in info["symbols"] if s["symbol"] in wanted]

    client = ch.get_client()
    ch.insert(client, "symbol_info", rows, tables.SYMBOL_INFO_COLS)
    print(f"market={MKT}  populated symbol_info for {len(rows)}/{len(wanted)} symbols", flush=True)


if __name__ == "__main__":
    main()
