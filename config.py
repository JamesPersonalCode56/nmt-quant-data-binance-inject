"""Configuration: load .env, auto-pick a reachable ClickHouse endpoint."""
from __future__ import annotations

import functools
import os
import time
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# --- ClickHouse endpoint candidates, in preference order ------------------
_HOST_CANDIDATES = ["127.0.0.1", "192.168.122.226", "100.115.36.121"]

# A transient CH-unreachable at startup must NOT crash the process into a Docker
# crash-loop: retry the /ping sweep for a bounded budget before giving up.
_CH_RESOLVE_ATTEMPTS = 10        # sweeps over the candidate list
_CH_RESOLVE_BACKOFF = 3.0        # seconds between sweeps (~30s total budget)

CH_HTTP_PORT = int(os.getenv("CH_HTTP_PORT", "8124"))
CH_NATIVE_PORT = int(os.getenv("CH_NATIVE_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "admin_nmt")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "crypto")

EXCHANGE = os.getenv("EXCHANGE", "binance")
MARKET_TYPE = os.getenv("MARKET_TYPE", "um")

# Which pairs each market collects now lives in pairs.yaml (see symbols.resolve).
DATASETS = [d.strip() for d in os.getenv("DATASETS", "trades,bookDepth,metrics").split(",") if d.strip()]

WORKERS = int(os.getenv("WORKERS", "8"))
INSERT_BATCH = int(os.getenv("INSERT_BATCH", "100000"))
L2_LEVELS = int(os.getenv("L2_LEVELS", "20"))
LIVE_GROUPS = int(os.getenv("LIVE_GROUPS", "6"))
KLINE_GROUPS = int(os.getenv("KLINE_GROUPS", "2"))           # WS kline processes (ohlcv)
OI_FUNDING_SECS = float(os.getenv("OI_FUNDING_SECS", "5.0")) # real-time OI + funding poll period
LIVE_FLUSH_ROWS = int(os.getenv("LIVE_FLUSH_ROWS", "50000")) # buffer high-water (memory cap)
LIVE_FLUSH_SECS = float(os.getenv("LIVE_FLUSH_SECS", "2.0")) # normal flush cadence

# --- Market-aware endpoints. MARKET_TYPE fixes the venue for this whole process. ----
_SPOT = MARKET_TYPE == "spot"

BINANCE_FAPI = "https://fapi.binance.com"     # USDM futures REST (also /futures/data, OI, funding)
BINANCE_SPOT = "https://api.binance.com"      # spot REST

# REST: aggTrades (live-trades fallback) + exchangeInfo (universe + symbol_info metadata).
AGGTRADES_URL     = f"{BINANCE_SPOT}/api/v3/aggTrades" if _SPOT else f"{BINANCE_FAPI}/fapi/v1/aggTrades"
EXCHANGE_INFO_URL = f"{BINANCE_SPOT}/api/v3/exchangeInfo" if _SPOT else f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
REST_WEIGHT_LIMIT = 1200 if _SPOT else 2400   # IP weight budget per minute

# Vision daily history. Spot exposes only `trades` (no bookDepth/metrics).
VISION_BASE = ("https://data.binance.vision/data/spot/daily" if _SPOT
               else "https://data.binance.vision/data/futures/um/daily")

# WebSocket bases. Futures splits market data by ROUTE: trade/price streams
# (@aggTrade, @markPrice, @kline) require the /market route; order-book/quote streams
# (@depth, @bookTicker) come from the unrouted/Public endpoint (an unrouted connection
# only ever delivers Public streams). Spot serves everything from one endpoint.
if _SPOT:
    WS_PUBLIC_BASE = "wss://stream.binance.com:9443/stream"
    WS_MARKET_BASE = WS_PUBLIC_BASE
else:
    WS_PUBLIC_BASE = "wss://fstream.binance.com/stream"          # @depth, @bookTicker
    WS_MARKET_BASE = "wss://fstream.binance.com/market/stream"   # @aggTrade, @markPrice, @kline

# Kline intervals = "all timeframes Binance provides" (spot adds 1s). Closed candles only.
_FUT_INTERVALS = "1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M"
_SPOT_INTERVALS = "1s," + _FUT_INTERVALS
KLINE_INTERVALS = [s.strip() for s in
                   os.getenv("KLINE_INTERVALS", _SPOT_INTERVALS if _SPOT else _FUT_INTERVALS).split(",")
                   if s.strip()]

# Optional SOCKS5/HTTP proxy for the Binance WS only (ClickHouse/REST stay direct).
# NOT needed from VN — /market works directly. Kept for flexibility.
WS_PROXY = os.getenv("WS_PROXY", "").strip() or None
# Live trades over WebSocket @aggTrade (default). Set WS_TRADES=false for REST pagination.
WS_TRADES = os.getenv("WS_TRADES", "true").strip().lower() in ("1", "true", "yes")


def _ping(host: str) -> bool:
    try:
        r = requests.get(f"http://{host}:{CH_HTTP_PORT}/ping", timeout=3)
        return r.status_code == 200 and r.text.strip() == "Ok."
    except Exception:
        return False


@functools.lru_cache(maxsize=1)
def resolve_ch_host() -> str:
    """Return CH_HOST from env, else the first candidate that answers /ping (cached).

    Retries the /ping sweep with backoff so a transient CH-unreachable at startup
    waits it out instead of crashing the process into a Docker crash-loop.
    """
    env_host = os.getenv("CH_HOST", "").strip()
    if env_host:
        return env_host
    for attempt in range(_CH_RESOLVE_ATTEMPTS):
        for h in _HOST_CANDIDATES:
            if _ping(h):
                return h
        if attempt < _CH_RESOLVE_ATTEMPTS - 1:
            time.sleep(_CH_RESOLVE_BACKOFF)
    raise RuntimeError(
        f"No reachable ClickHouse on :{CH_HTTP_PORT} among {_HOST_CANDIDATES} "
        f"after {_CH_RESOLVE_ATTEMPTS} attempts. Set CH_HOST in .env."
    )


def backfill_range() -> tuple[date, date]:
    """(start, end) inclusive. Defaults: last 30 days ending today-1 (Vision lags ~1-2d)."""
    end_s = os.getenv("END_DATE", "").strip()
    start_s = os.getenv("START_DATE", "").strip()
    end = date.fromisoformat(end_s) if end_s else (date.today() - timedelta(days=1))
    start = date.fromisoformat(start_s) if start_s else (end - timedelta(days=29))
    return start, end
