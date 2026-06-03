"""Real-time open interest + funding rate (futures only).

  -> crypto.open_interest   (current OI quantity + value)
  -> crypto.funding_rate    (current/predicted funding rate, mark/index price)

Neither has a clean low-latency snapshot WS, so we poll REST every OI_FUNDING_SECS:
  * /fapi/v1/premiumIndex (ALL symbols in ONE call) -> mark price, index price, current
    funding rate, next funding time. Drives funding_rate, keyed by the NEXT funding ts so
    ReplacingMergeTree converges to the realized rate by settlement; also supplies mark
    price for the OI value.
  * /fapi/v1/openInterest?symbol= (per symbol) -> current open interest quantity.

open_interest_value = qty * mark_price; oi_currency = base asset (USDM OI is in base units).
"""
from __future__ import annotations

import time

import requests

import ch
import config
import tables
from util import ms_to_dt, now_utc, dec

_PREMIUM = f"{config.BINANCE_FAPI}/fapi/v1/premiumIndex"
_OI = f"{config.BINANCE_FAPI}/fapi/v1/openInterest"
_FUND_INFO = f"{config.BINANCE_FAPI}/fapi/v1/fundingInfo"
_EXINFO = f"{config.BINANCE_FAPI}/fapi/v1/exchangeInfo"
MKT, EXC = config.MARKET_TYPE, config.EXCHANGE


def _base_assets(sess: requests.Session, symbols: list[str]) -> dict[str, str]:
    try:
        info = sess.get(_EXINFO, timeout=30).json()
        m = {s["symbol"]: s.get("baseAsset", "") for s in info.get("symbols", [])}
    except Exception:
        m = {}
    return {s: m.get(s, "") for s in symbols}


def _funding_intervals(sess: requests.Session, symbols: list[str]) -> dict[str, int]:
    out = {s: 8 for s in symbols}   # USDT perps default to 8h funding
    try:
        for e in sess.get(_FUND_INFO, timeout=15).json():
            if e.get("symbol") in out and e.get("fundingIntervalHours"):
                out[e["symbol"]] = int(e["fundingIntervalHours"])
    except Exception:
        pass
    return out


def run(symbols: list[str], seconds: int = 0) -> None:
    client = ch.get_client()
    sess = requests.Session()
    sset = set(symbols)
    base = _base_assets(sess, symbols)
    fiv = _funding_intervals(sess, symbols)
    deadline = time.time() + seconds if seconds else None

    while True:
        ing = now_utc()
        oi_rows, fr_rows = [], []
        try:
            arr = sess.get(_PREMIUM, timeout=15).json()
            prem = {d["symbol"]: d for d in arr if d.get("symbol") in sset}
        except Exception as e:  # noqa: BLE001
            prem = {}
            print(f"[oi_funding] premiumIndex error: {e}", flush=True)

        for s in symbols:
            p = prem.get(s, {})
            mark = dec(p.get("markPrice"))
            if p:
                nft = ms_to_dt(p["nextFundingTime"]) if p.get("nextFundingTime") else None
                rate = dec(p.get("lastFundingRate"))
                src = ms_to_dt(p["time"]) if p.get("time") else ing
                fr_rows.append((
                    EXC, MKT, s, (nft or ing), rate, fiv.get(s, 8),
                    mark, dec(p.get("indexPrice")), nft, rate,
                    src, ing, {"src": "premiumIndex"},
                ))
            try:
                d = sess.get(_OI, params={"symbol": s}, timeout=15).json()
                qty = dec(d.get("openInterest"))
            except Exception as e:  # noqa: BLE001
                print(f"[oi_funding] OI {s} error: {e}", flush=True)
                continue
            if qty is not None:
                ts = ms_to_dt(d["time"]) if d.get("time") else ing
                val = (qty * mark) if mark is not None else None
                oi_rows.append((
                    EXC, MKT, s, ts, qty, val, base.get(s, ""), mark, ts, ing,
                    {"src": "openInterest"},
                ))

        if fr_rows:
            ch.insert(client, "funding_rate", fr_rows, tables.FUNDING_RATE_COLS)
        if oi_rows:
            ch.insert(client, "open_interest", oi_rows, tables.OPEN_INTEREST_COLS)
        print(f"[oi_funding] oi={len(oi_rows)} funding={len(fr_rows)}", flush=True)

        if deadline and time.time() >= deadline:
            return
        end = time.time() + config.OI_FUNDING_SECS
        while time.time() < end:
            if deadline and time.time() >= deadline:
                return
            time.sleep(min(1.0, max(0.0, end - time.time())))
