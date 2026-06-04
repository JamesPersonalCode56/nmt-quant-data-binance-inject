# tick-crawler — Binance tick + market data → ClickHouse (`crypto`)

Kéo dữ liệu Binance (**USDM futures** + **spot**) cho HFT vào warehouse ClickHouse dùng chung
(`crypto`). Điền **toàn bộ 9 bảng**: 5 bảng tick mới + 4 bảng cũ (`symbol_info`, `ohlcv`,
`open_interest`, `funding_rate`) — bằng `CREATE … IF NOT EXISTS` và `populate`, **không bao
giờ `ALTER`** bảng cũ.

- **Backfill (quá khứ)** — `data.binance.vision`: futures `trades`/`bookDepth`/`metrics`; spot `trades`.
- **Live (real-time)** — WebSocket order book + `@aggTrade` + `@kline` (mọi khung, nến đã đóng)
  + REST open-interest/funding-rate + REST long/short metrics.

> Dependency manager: **Poetry**, virtualenv **in-project** (`.venv/`). Chạy **cùng máy với
> ClickHouse** (endpoint tự nhận `127.0.0.1`). Tune cho **8 CPU** dùng chung. Trades chạy
> thẳng từ VN, **không cần VPN** (route `/market`).

## Hai instance (MARKET_TYPE cố định venue cho cả process)
| Instance | `MARKET_TYPE` | Symbol (khai báo trong `pairs.yaml`) | Thu thập |
|---|---|---|---|
| futures | `um` (mặc định) | `markets.um`: scope `demo` (~46) + extra PAXG/XAUT | order book, trades, ohlcv, **open_interest, funding_rate, metrics** |
| spot | `spot` | `markets.spot`: PAXGUSDT, XAUTUSDT | order book, trades, ohlcv (có cả `1s`) |

OI / funding / long-short metrics là **futures-only** (Binance không cung cấp cho spot) → instance
spot tự bỏ qua. Cả hai ghi chung bảng, phân biệt bằng cột `market_type`.

> **Đổi cặp thu thập:** sửa `pairs.yaml` (`markets.<um|spot>.scope` / `.extra`) — **không** còn
> `SYMBOLS_SCOPE`/`EXTRA_SYMBOLS` trong env. `scope` nhận: tên universe (`demo`/`core`), `all`
> (mọi cặp USDT trên venue), hoặc list cụ thể. `MARKET_TYPE` chỉ chọn venue + section YAML.
> Override 1 lần chạy: cờ `--symbols`.

## Cài & chạy
```bash
cd HFT/research/data/tick-crawler
poetry install                       # tạo .venv/ + cài deps
cp .env.example .env                 # điền CH_PASSWORD

poetry run python migrate.py --recreate   # tạo 5 bảng tick mới (kèm CODEC nén)
poetry run python symbol_info.py          # điền metadata symbol_info (futures universe)
MARKET_TYPE=spot poetry run python symbol_info.py   # metadata cho spot (cặp lấy từ pairs.yaml)

# Backfill lịch sử (idempotent qua ingest_state + ReplacingMergeTree)
poetry run python -m backfill.main                        # futures, range trong .env
MARKET_TYPE=spot poetry run python -m backfill.main       # spot (cặp từ pairs.yaml, chỉ trades)

# Live 24/7
poetry run python -m live.main                            # futures: đủ 5 nguồn
MARKET_TYPE=spot poetry run python -m live.main --groups 1   # spot (cặp từ pairs.yaml)
# smoke: thêm --seconds 60
```

### Backfill định kỳ — chạy bao lâu một lần (QUAN TRỌNG)

Live **không** thay thế được backfill. Hai thứ chỉ backfill mới có:
- **`book_depth`** (±1..5% depth, mỗi 5s) — **không có nguồn live**, chỉ Vision cấp. Không backfill ⇒ bảng này **rỗng vĩnh viễn**.
- **Lịch sử trước khi live khởi động** + `trades` full per-trade id (live chỉ có aggTrade từ lúc bật).

`data.binance.vision` xuất file **theo ngày, trễ ~1–2 ngày**. Backfill **idempotent** (skip task `done` trong `ingest_state` + ReplacingMergeTree dedupe), nên chạy lại rất rẻ — chỉ tải phần còn thiếu.

**Khuyến nghị: chạy backfill HẰNG NGÀY** cho cửa sổ trailing vài ngày (bù độ trễ Vision), tối thiểu **hàng tuần** nếu không cần `book_depth` liên tục. Ví dụ cron (chạy 06:00 mỗi ngày, backfill 3 ngày gần nhất):

```bash
# crontab -e
0 6 * * * cd /…/HFT/research/data/tick-crawler && \
  START_DATE=$(date -u -d '3 days ago' +\%F) END_DATE=$(date -u -d '1 day ago' +\%F) \
  poetry run python -m backfill.main >> backfill.log 2>&1
```

> Lần đầu (điền lịch sử): bỏ `START_DATE/END_DATE` để dùng mặc định **30 ngày gần nhất** trong `config.backfill_range()`. Lưu ý dung lượng: 1 ngày `trades` BTCUSDT ~1.7M dòng.

## Docker Compose (2 service, KHÔNG cần VPN)
```bash
cp .env.example .env
docker compose run --rm crawler python migrate.py --recreate
docker compose run --rm crawler python symbol_info.py
docker compose run --rm crawler-spot python symbol_info.py
docker compose up -d crawler crawler-spot     # live cả futures + spot
docker compose logs -f crawler
```
Cả hai `network_mode: host` (tới ClickHouse `127.0.0.1`). `crawler-spot` override `MARKET_TYPE=spot`.

## Kết nối ClickHouse
Auto-pick `CH_HOST`: `127.0.0.1` → `192.168.122.226` (NAT) → `100.115.36.121` (Tailscale);
trên datalake VM chọn `127.0.0.1` ngay. Client HTTP 8124 (`clickhouse-connect`). Creds trong `.env`.

## Bảng (`migrate.py` tạo 5 bảng mới; `symbol_info.py`/live điền 4 bảng cũ)
| Bảng | Loại | Nguồn |
|---|---|---|
| `trades` | mới | Vision `trades` + live WS `@aggTrade` (`/market`); `extra.src`=`vision`/`ws_agg`/`rest_agg` |
| `book_depth` | mới | Vision `bookDepth` (±1..5% mỗi 5s) |
| `book_snapshot_l2` | mới | live WS `@depthN@100ms` (L2 top-N, mặc định 20) |
| `futures_metrics` | mới | Vision `metrics` + live REST `/futures/data/*` (long/short ratios, 5m) |
| `ingest_state` | mới | registry idempotent cho backfill |
| **`symbol_info`** | cũ | `exchangeInfo` (tick/step/precision/notional/status…) — 1 dòng/symbol |
| **`ohlcv`** | cũ | live WS `@kline_<iv>` **mọi khung, chỉ nến đã đóng** (`is_final=1`) |
| **`open_interest`** | cũ | live REST `/fapi/v1/openInterest` (+ mark từ `premiumIndex`) real-time |
| **`funding_rate`** | cũ | live REST `/fapi/v1/premiumIndex` real-time (rate + mark/index + next funding) |

Tất cả `ReplacingMergeTree` → chạy lại / overlap không tạo bản trùng. `funding_rate` keyed theo
`next_funding_ts` nên các lần poll trong 1 chu kỳ funding **hội tụ về 1 dòng** (rate dự đoán → rate thực).

### Nén (CODEC — chỉ áp cho 5 bảng mới, qua `migrate.py --recreate`)
Tick data tune codec riêng: `trade_id`/timestamp tăng dần → `DoubleDelta, ZSTD`; Decimal → `ZSTD`.
Giảm ~50–60% so với LZ4 mặc định (vd `trade_id` từ 2.0x lên ~rất cao nhờ DoubleDelta).

## Live — kiến trúc tiến trình (8 CPU)
- **Order book**: `LIVE_GROUPS` process, mỗi cái 1 WS combined `@depth20@100ms` (route Public).
- **Trades**: 1 WS `@aggTrade` (route `/market` cho futures) → fallback REST `--rest-trades`.
- **Klines/ohlcv**: `KLINE_GROUPS` process, combined `@kline_<iv>` mọi khung (route `/market`),
  **chỉ insert nến đã đóng** (`k.x==true`).
- **OI + funding** (futures): 1 REST poller mỗi `OI_FUNDING_SECS` (premiumIndex all-symbols 1 call + openInterest/symbol).
- **Metrics** (futures): 1 REST poller `/futures/data/*` mỗi 5 phút.

Cờ tắt từng phần: `--no-orderbook --no-trades --no-klines --no-oi-funding --no-metrics`.

### Xoay vòng WebSocket 24h (không mất data)
`live/wsmanager.py` dùng **make-before-break**: mở connection thay thế ~15s trước mốc 24h, chạy
song song rồi mới bỏ cái cũ; event trùng bị ReplacingMergeTree gộp → không gap, không trùng. Tự
reconnect khi rớt hoặc "im" >30s. (`ping_interval=None` — thư viện tự pong ping của Binance.)

## ⚠️ Routing WS của Binance USDM
Futures tách market-data WS theo **route**: `@aggTrade`/`@markPrice`/`@kline` **phải** dùng base
`/market`; `@depth`/`@bookTicker` dùng route Public (unrouted). Connection unrouted **chỉ nhận
Public** — nên `@aggTrade`/`@kline` trên `/stream` cũ trả 0 frame (nhìn như bị "chặn vùng", KHÔNG
phải). Dùng đúng `/market` thì chảy **thẳng từ VN, không VPN**. Spot phục vụ mọi stream từ 1 endpoint
(`wss://stream.binance.com:9443`); payload depth của spot **không kèm symbol/time** → lấy symbol từ
tên stream, timestamp = lúc nhận. Code dùng `config.WS_PUBLIC_BASE` / `config.WS_MARKET_BASE`.

## Bảo mật
- `CH_PASSWORD` chỉ trong `.env` (gitignore). `.env.example` để trống.
- Backfill `trades` 1 ngày BTCUSDT ~1.7M dòng; cân nhắc dung lượng khi chạy full nhiều symbol × ngày.
