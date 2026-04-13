#!/usr/bin/env python3
"""
bse_stock_harvester.py
======================
Standalone BSE Cash (BC) tick harvester extracted from monolithic tick_harvester.py.

Design goals:
- BC-only master resolution (no NC/NF/BF calls)
- Zero-pandas, CSV batch writer (RAM-safe)
- WebSocket auto-reconnect with exponential backoff
- Periodic safety flush
- Ready for low-memory EC2 instances
"""

from __future__ import annotations

import csv
import json
import logging
import os
import signal
import threading
import time
import zlib
from datetime import date, datetime, timedelta
from pathlib import Path

from SharekhanApi.sharekhanConnect import SharekhanConnect  # type: ignore
from SharekhanApi.sharekhanWebsocket import SharekhanWebSocket  # type: ignore


# ============================================================================
# Configuration
# ============================================================================
_BASE_DIR = Path(__file__).parent

CONFIG_FILE = _BASE_DIR / "config.json"
FO_SYMBOLS_FILE = _BASE_DIR / "fo_symbols.txt"
SECTOR_SYMBOLS_FILE = _BASE_DIR / "sector_symbols.json"
NSE_HOLIDAYS_FILE = _BASE_DIR / "nse_holidays.txt"
NSE_HOLIDAYS_FILE_FALLBACK = _BASE_DIR.parent / "nse_holidays.txt"

# Strict requirement from user:
DATA_DIR = _BASE_DIR / "tick_data"
LOG_FILE = _BASE_DIR / "bse_stock_harvester.log"

EXCHANGE_BSE = "BC"
EXCHANGE_TO_WRITER_KEY = {
    EXCHANGE_BSE: "bse_stock",
}

BATCH_SIZE = 500
RECONNECT_DELAY_MIN = 5
RECONNECT_DELAY_MAX = 60
PERIODIC_FLUSH_INTERVAL = 60

WS_FEED_CHUNK_SIZE = 500
WS_FEED_CHUNK_DELAY_SEC = 0.35
WS_FEED_HANDSHAKE_PAUSE_SEC = 0.50

MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 15  # 3 PM
MARKET_CLOSE_MINUTE = 45  # 3:45 PM

INSTRUMENT_CODES: list[str] = []
CODE_TO_SYMBOL: dict[str, str] = {}

CSV_HEADERS: list[str] = [
    "Timestamp",
    "Exchange",
    "Symbol",
    "LTP",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "VWAP",
    "Best_Bid",
    "Bid_Qty",
    "Best_Ask",
    "Ask_Qty",
    "OI",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("bse_stock_harvester")


def _load_nse_holidays() -> frozenset[date]:
    """
    Load holiday dates from nse_holidays.txt (DD-MM-YYYY per line).
    Falls back to parent folder if the file is not present beside this script.
    """
    holiday_file = NSE_HOLIDAYS_FILE
    if not holiday_file.exists() and NSE_HOLIDAYS_FILE_FALLBACK.exists():
        holiday_file = NSE_HOLIDAYS_FILE_FALLBACK

    holidays: set[date] = set()

    if not holiday_file.exists():
        log.warning("Holiday file not found at %s; weekend-only trading-day guard will be used.", NSE_HOLIDAYS_FILE)
        return frozenset()

    try:
        with open(holiday_file, "r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                date_part = line.split()[0]
                try:
                    holidays.add(datetime.strptime(date_part, "%d-%m-%Y").date())
                except ValueError:
                    log.warning(
                        "nse_holidays.txt line %d: could not parse date '%s'; skipped.",
                        lineno,
                        date_part,
                    )
    except OSError as exc:
        log.warning("Could not read holiday file %s: %s", holiday_file, exc)
        return frozenset()

    log.info("Loaded %d holiday(s) from %s", len(holidays), holiday_file)
    return frozenset(holidays)


NSE_HOLIDAYS: frozenset[date] = _load_nse_holidays()


# ============================================================================
# Config and symbol loading
# ============================================================================
def _extract_access_token(raw: object) -> str:
    """Normalize access_token from either plain string or dict payload."""
    if isinstance(raw, str):
        return raw.strip()

    if isinstance(raw, dict):
        for key in ("token", "access_token", "value", "jwtToken", "jwt_token"):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    return ""


def load_config() -> dict:
    """Load runtime config from config.json with environment overrides."""
    cfg: dict = {}

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Could not read config.json: %s - falling back to env vars.", exc)

    cfg["api_key"] = os.getenv("SHAREKHAN_API_KEY", str(cfg.get("api_key", ""))).strip()

    env_access = os.getenv("SHAREKHAN_ACCESS_TOKEN", "").strip()
    cfg_access = _extract_access_token(cfg.get("access_token", ""))
    cfg["access_token"] = env_access or cfg_access

    if not cfg.get("api_key"):
        raise ValueError("api_key is missing. Fill config.json or set SHAREKHAN_API_KEY.")
    if not cfg.get("access_token"):
        raise ValueError(
            "access_token is missing. Run auth_helper.py to refresh and save it in config.json."
        )

    return cfg


def _load_sector_symbols(sector_file: Path) -> set[str]:
    """Load optional sector_symbols.json and flatten all symbols."""
    if not sector_file.exists():
        log.info("sector_symbols.json not found at %s - using fo_symbols.txt only.", sector_file)
        return set()

    try:
        with open(sector_file, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {sector_file}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"{sector_file} must map sector names to symbol arrays.")

    symbols: set[str] = set()
    for sector_name, values in payload.items():
        if not isinstance(values, list):
            raise ValueError(f"sector '{sector_name}' must contain a JSON array.")
        for item in values:
            symbol = str(item).strip().upper()
            if symbol:
                symbols.add(symbol)

    log.info("sector_symbols.json loaded: %d symbol(s).", len(symbols))
    return symbols


def _load_target_symbols(symbols_file: Path) -> set[str]:
    """Load base fo_symbols.txt and merge with optional sector symbols."""
    if not symbols_file.exists():
        raise FileNotFoundError(
            f"fo_symbols.txt not found at {symbols_file}. Create it with one symbol per line."
        )

    base: set[str] = set()
    with open(symbols_file, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            base.add(line.upper())

    if not base:
        raise ValueError(f"{symbols_file} is empty. Add at least one symbol.")

    extra = _load_sector_symbols(SECTOR_SYMBOLS_FILE)
    merged = base | extra
    log.info("Target symbols loaded: base=%d, extra=%d, total=%d", len(base), len(extra), len(merged))
    return merged


# ============================================================================
# Master response helpers and BC-only scrip resolver
# ============================================================================
def _normalise_master_response(raw: object, exchange: str) -> list[dict]:
    """Normalize Sharekhan master() responses into a list of dict rows."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.error("master(%s): JSON decode failed: %s", exchange, exc)
            return []

    if raw is None:
        log.error("master(%s) returned None.", exchange)
        return []

    if isinstance(raw, list):
        return raw

    if isinstance(raw, dict):
        for key in ("data", "result", "records", "scriptMaster", "ScriptMaster"):
            if isinstance(raw.get(key), list):
                return raw[key]
        log.error("master(%s): unexpected dict shape, keys=%s", exchange, list(raw.keys())[:10])
        return []

    log.error("master(%s): unsupported response type %s", exchange, type(raw).__name__)
    return []


def _get_field(record: dict, *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def fetch_dynamic_scrip_codes(
    sharekhan: SharekhanConnect,
    symbols_file: Path = FO_SYMBOLS_FILE,
) -> list[str]:
    """
    Resolve BSE Cash (BC) scrip codes for all symbols from fo_symbols + sector file.

    Strictly BC only:
    - No NC fetch
    - No NF/BF fetch
    """
    target_symbols = _load_target_symbols(symbols_file)

    log.info("Fetching BSE Cash master (exchange=BC) for %d symbol(s)...", len(target_symbols))
    try:
        raw_bc = sharekhan.master(EXCHANGE_BSE)
    except Exception as exc:
        raise RuntimeError(f"sharekhan.master('BC') failed: {exc}") from exc

    records = _normalise_master_response(raw_bc, EXCHANGE_BSE)
    log.info("BC master rows received: %d", len(records))

    instrument_codes: list[str] = []
    matched_symbols: set[str] = set()

    for record in records:
        sym_raw = _get_field(
            record,
            "TradingSymbol", "tradingSymbol", "tradingsymbol",
            "TRADINGSYMBOL", "Symbol", "symbol", "Name", "name",
        )
        if not sym_raw:
            continue

        symbol = sym_raw.upper()
        if symbol not in target_symbols:
            continue

        code = _get_field(
            record,
            "ScripCode", "scripCode", "scripcode", "SCRIPCODE",
            "Scripcode", "Code", "code", "scripId", "ScripId",
        )
        if not code:
            log.warning("BC: found %s but missing scrip code; skipping.", symbol)
            continue

        instrument = f"{EXCHANGE_BSE}{code}"
        instrument_codes.append(instrument)
        CODE_TO_SYMBOL[instrument] = symbol
        matched_symbols.add(symbol)

    unresolved = target_symbols - matched_symbols
    if unresolved:
        log.warning(
            "Unresolved symbols in BC master (%d): %s",
            len(unresolved),
            ", ".join(sorted(unresolved)),
        )

    # Deduplicate while preserving order.
    seen: set[str] = set()
    unique_codes: list[str] = []
    for code in instrument_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    if not unique_codes:
        raise RuntimeError("BC scrip resolution returned 0 instruments.")

    log.info("BC dynamic scrip resolution complete: %d instrument code(s).", len(unique_codes))
    return unique_codes


# ============================================================================
# CSV writer
# ============================================================================
class CsvBatchWriter:
    """Thread-safe in-memory batch writer with symbol-wise CSV files."""

    def __init__(self, data_dir: Path, batch_size: int = BATCH_SIZE) -> None:
        self._dir = data_dir
        self._batch_size = batch_size
        self._buffer: dict[str, list[dict]] = {}
        self._lock = threading.Lock()
        self._current_date = date.today()
        self._dir.mkdir(parents=True, exist_ok=True)
        log.info("CsvBatchWriter ready: dir=%s batch=%d", self._dir, self._batch_size)

    def _csv_path(self, symbol: str) -> Path:
        today = datetime.now().date()
        day_str = datetime.now().strftime("%d-%m-%Y")

        if today != self._current_date:
            log.info(
                "Midnight rollover: %s -> %s",
                self._current_date.strftime("%d-%m-%Y"),
                day_str,
            )
            self._current_date = today

        day_dir = self._dir / day_str
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir / f"{symbol}.csv"

    def _flush_symbol_locked(self, symbol: str) -> None:
        ticks = self._buffer.get(symbol)
        if not ticks:
            return

        target = self._csv_path(symbol)
        file_existed = target.exists()
        n = len(ticks)

        try:
            with open(target, "a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS, extrasaction="ignore")
                if not file_existed:
                    writer.writeheader()
                writer.writerows(ticks)
            ticks.clear()
            log.debug("Flushed %d ticks -> %s", n, target)
        except OSError as exc:
            log.error("Write failed (%s). Buffer retained (%d items).", exc, n)

    def _flush_locked(self) -> None:
        for symbol in list(self._buffer):
            self._flush_symbol_locked(symbol)

    def add(self, tick: dict) -> None:
        symbol = tick.get("Symbol") or "UNKNOWN"
        with self._lock:
            bucket = self._buffer.setdefault(symbol, [])
            bucket.append(tick)
            if len(bucket) >= self._batch_size:
                self._flush_symbol_locked(symbol)

    def flush(self) -> None:
        with self._lock:
            count = sum(len(v) for v in self._buffer.values())
            self._flush_locked()
            if count:
                log.info("Safety flush wrote %d buffered ticks.", count)

    def __len__(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._buffer.values())


# ============================================================================
# Tick parser
# ============================================================================
def _parse_inner_tick(inner: dict) -> dict | None:
    raw_exchange = inner.get("exchangeCode") or inner.get("exchange")
    exchange = raw_exchange if isinstance(raw_exchange, str) else None
    scrip = inner.get("scripCode")

    if exchange and scrip is not None:
        symbol = CODE_TO_SYMBOL.get(f"{exchange}{scrip}")
        if symbol is None:
            symbol = str(scrip)
    else:
        symbol = inner.get("tradingSymbol") or inner.get("symbol")

    if symbol is None:
        return None

    return {
        "Timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "Exchange": exchange,
        "Symbol": symbol,
        "LTP": inner.get("ltp") or inner.get("lastTradedPrice") or None,
        "Open": inner.get("open") or inner.get("openPrice") or None,
        "High": inner.get("high") or inner.get("highPrice") or None,
        "Low": inner.get("low") or inner.get("lowPrice") or None,
        "Close": inner.get("close") or inner.get("closePrice") or None,
        "Volume": inner.get("qty") or inner.get("volume") or inner.get("totalTradeQty") or None,
        "VWAP": inner.get("avgPrice") or inner.get("avgTradedPrice") or inner.get("atp") or None,
        "Best_Bid": inner.get("bidPrice") or inner.get("bestBidPrice") or None,
        "Bid_Qty": inner.get("bidQty") or inner.get("bestBidQty") or inner.get("bidQuantity") or None,
        "Best_Ask": inner.get("offPrice") or inner.get("bestAskPrice") or inner.get("bestOfferPrice") or None,
        "Ask_Qty": inner.get("offQty") or inner.get("bestAskQty") or inner.get("bestOfferQty") or inner.get("askQuantity") or None,
        "OI": inner.get("currentOI") or inner.get("oi") or inner.get("openInterest") or None,
    }


def parse_tick(raw_message: object) -> list[dict] | None:
    try:
        if raw_message is None or raw_message in ("pong", "heartbeat"):
            return None

        envelope: dict = json.loads(raw_message) if isinstance(raw_message, str) else raw_message
        if not isinstance(envelope, dict):
            return None

        if envelope.get("message") != "feed":
            return None

        payload = envelope.get("data")
        if payload is None:
            return None

        if isinstance(payload, list):
            ticks = [_parse_inner_tick(item) for item in payload if isinstance(item, dict)]
            ticks = [tick for tick in ticks if tick is not None]
            return ticks or None

        if isinstance(payload, dict):
            tick = _parse_inner_tick(payload)
            return [tick] if tick else None

        return None

    except (json.JSONDecodeError, AttributeError, TypeError) as exc:
        log.warning("Tick parse error: %s | raw=%s", exc, str(raw_message)[:200])
        return None


# ============================================================================
# Periodic flusher
# ============================================================================
class PeriodicFlusher(threading.Thread):
    def __init__(self, writer: CsvBatchWriter, interval: int = PERIODIC_FLUSH_INTERVAL) -> None:
        super().__init__(name="periodic-flusher", daemon=True)
        self._writer = writer
        self._interval = interval
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        log.info("PeriodicFlusher started (interval=%ds).", self._interval)
        while not self._stop_evt.wait(timeout=self._interval):
            self._writer.flush()
        log.info("PeriodicFlusher stopped.")


# ============================================================================
# Trading-day guard (weekend + holiday file)
# ============================================================================
def _is_trading_day(d: date) -> bool:
    return d.weekday() not in (5, 6) and d not in NSE_HOLIDAYS


def _next_trading_day(from_date: date) -> date:
    probe = from_date + timedelta(days=1)
    while not _is_trading_day(probe):
        probe += timedelta(days=1)
    return probe


def _market_open_dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(hours=MARKET_OPEN_HOUR)


def _market_close_dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(hours=MARKET_CLOSE_HOUR, minutes=MARKET_CLOSE_MINUTE)


def _is_market_session_now(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if not _is_trading_day(now.date()):
        return False
    return _market_open_dt(now.date()) <= now < _market_close_dt(now.date())


def _sleep_until_market_session() -> None:
    now = datetime.now()
    if _is_market_session_now(now):
        return

    today = now.date()
    if _is_trading_day(today) and now < _market_open_dt(today):
        wake_dt = _market_open_dt(today)
        reason = "before market open"
    else:
        next_day = _next_trading_day(today)
        wake_dt = _market_open_dt(next_day)
        reason = "after market close" if _is_trading_day(today) else "holiday/weekend"

    wait_secs = max(0.0, (wake_dt - now).total_seconds())

    log.info(
        "Outside market session (%s). Sleeping until %s (~%.1f hours).",
        reason,
        wake_dt.strftime("%Y-%m-%d %H:%M:%S"),
        wait_secs / 3600.0,
    )

    deadline = time.monotonic() + wait_secs
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        time.sleep(min(60.0, remaining))


# ============================================================================
# Harvester engine
# ============================================================================
class TickHarvester:
    def __init__(self, access_token: str, writer: CsvBatchWriter, instrument_codes: list[str]) -> None:
        self._access_token = access_token
        self._writer = writer
        self._instrument_codes = instrument_codes
        self._sws: SharekhanWebSocket | None = None
        self._session_close_timer: threading.Timer | None = None
        self._shutdown = threading.Event()
        self._reconnect_delay = RECONNECT_DELAY_MIN

    def is_shutdown_requested(self) -> bool:
        return self._shutdown.is_set()

    def _arm_session_close_timer(self) -> None:
        if self._session_close_timer is not None:
            self._session_close_timer.cancel()
            self._session_close_timer = None

        close_in_secs = (_market_close_dt(date.today()) - datetime.now()).total_seconds()
        if close_in_secs <= 0:
            return

        def _close_ws_at_session_end() -> None:
            if self._shutdown.is_set():
                return
            log.info("Market close reached (16:00). Closing socket and sleeping until next session.")
            if self._sws is not None:
                try:
                    self._sws.close_connection()
                except Exception as exc:
                    log.debug("Session-end close_connection() raised (ignored): %s", exc)

        timer = threading.Timer(close_in_secs + 1.0, _close_ws_at_session_end)
        timer.daemon = True
        timer.start()
        self._session_close_timer = timer

    def request_shutdown(self, signum: int = 0, _frame: object = None) -> None:
        try:
            sig_name = signal.Signals(signum).name if signum else "programmatic"
        except ValueError:
            sig_name = str(signum)

        buffered = len(self._writer)
        log.info("Shutdown requested (%s). Flushing %d buffered ticks...", sig_name, buffered)
        self._shutdown.set()

        if self._session_close_timer is not None:
            self._session_close_timer.cancel()
            self._session_close_timer = None

        if self._sws is not None:
            try:
                self._sws.close_connection()
            except Exception as exc:
                log.debug("close_connection() raised (ignored): %s", exc)

        self._writer.flush()
        log.info("Flush complete. Exiting worker.")

    def _on_open(self, _wsapp: object) -> None:
        log.info("WebSocket connected. Subscribing to %d BC instrument(s)...", len(self._instrument_codes))
        self._reconnect_delay = RECONNECT_DELAY_MIN

        subscribe_msg = {
            "action": "subscribe",
            "key": ["feed"],
            "value": [""],
        }
        self._sws.subscribe(subscribe_msg)  # type: ignore[union-attr]
        time.sleep(WS_FEED_HANDSHAKE_PAUSE_SEC)

        chunks = [
            self._instrument_codes[i:i + WS_FEED_CHUNK_SIZE]
            for i in range(0, len(self._instrument_codes), WS_FEED_CHUNK_SIZE)
        ]

        for idx, chunk in enumerate(chunks, start=1):
            feed_msg = {
                "action": "feed",
                "key": ["ltp"],
                "value": [",".join(chunk)],
            }
            self._sws.fetchData(feed_msg)  # type: ignore[union-attr]
            if idx % 5 == 0 or idx == len(chunks):
                log.info("Feed subscription progress: %d/%d chunk(s).", idx, len(chunks))
            if idx < len(chunks):
                time.sleep(WS_FEED_CHUNK_DELAY_SEC)

        log.info(
            "Live feed subscription sent for %d BC instrument(s) in %d chunk(s).",
            len(self._instrument_codes),
            len(chunks),
        )
        self._arm_session_close_timer()

    def _on_data(self, _wsapp: object, message: object) -> None:
        ticks = parse_tick(message)
        if not ticks:
            return

        for tick in ticks:
            exchange = str(tick.get("Exchange") or "").upper().strip()
            writer_key = EXCHANGE_TO_WRITER_KEY.get(exchange)
            if writer_key != "bse_stock":
                continue
            self._writer.add(tick)

    def _on_error(self, _wsapp: object, error: object) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, _wsapp: object, *_args: object) -> None:
        log.warning("WebSocket closed by server or network.")

    def run(self) -> None:
        log.info(
            "BSE worker starting. instruments=%d batch=%d output=%s",
            len(self._instrument_codes),
            BATCH_SIZE,
            DATA_DIR,
        )

        attempt = 0
        while not self._shutdown.is_set():
            if not _is_market_session_now():
                log.info("Outside market session window (09:00-16:00). Returning to scheduler.")
                break

            attempt += 1
            log.info("Connection attempt #%d (next_backoff=%ds)", attempt, self._reconnect_delay)

            try:
                self._sws = SharekhanWebSocket(self._access_token)

                def _fixed_parse_binary(data: object) -> object:
                    if isinstance(data, (bytes, bytearray)):
                        try:
                            return zlib.decompress(data).decode("utf-8")
                        except Exception:
                            pass
                        try:
                            return data.decode("utf-8")
                        except Exception:
                            pass
                        return None
                    return data

                self._sws._parse_binary_data = _fixed_parse_binary  # type: ignore[attr-defined]
                self._sws.on_open = self._on_open  # type: ignore[assignment]
                self._sws.on_data = self._on_data  # type: ignore[assignment]
                self._sws.on_error = self._on_error  # type: ignore[assignment]
                self._sws.on_close = self._on_close  # type: ignore[assignment]

                self._sws.connect()

            except KeyboardInterrupt:
                log.info("KeyboardInterrupt received; initiating graceful shutdown.")
                self.request_shutdown()
                break

            except Exception as exc:
                log.error("Unhandled connect() exception on attempt #%d: %s", attempt, exc, exc_info=True)

            if self._shutdown.is_set():
                break

            if not _is_market_session_now():
                log.info("Market session ended. Returning to scheduler for overnight sleep.")
                break

            log.info("Disconnected. Retrying in %ds...", self._reconnect_delay)
            for _ in range(self._reconnect_delay):
                if self._shutdown.is_set():
                    break
                time.sleep(1)

            self._reconnect_delay = min(self._reconnect_delay * 2, RECONNECT_DELAY_MAX)

        log.info("Reconnect loop exited after %d attempt(s).", attempt)


# ============================================================================
# Entry point
# ============================================================================
def main() -> None:
    log.info("=" * 60)
    log.info("BSE Stock Harvester starting")
    log.info("=" * 60)

    try:
        cfg = load_config()
    except ValueError as exc:
        log.critical("Configuration error: %s", exc)
        raise SystemExit(1) from exc

    log.info("Config loaded.")

    rest_client = SharekhanConnect(cfg["api_key"], cfg["access_token"])

    try:
        instrument_codes = fetch_dynamic_scrip_codes(
            sharekhan=rest_client,
            symbols_file=FO_SYMBOLS_FILE,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        log.critical("BC scrip resolution failed: %s", exc)
        raise SystemExit(1) from exc

    INSTRUMENT_CODES[:] = instrument_codes

    log.info(
        "Total BC instruments: %d. First 10: %s%s",
        len(instrument_codes),
        ", ".join(instrument_codes[:10]),
        " ..." if len(instrument_codes) > 10 else "",
    )

    writer = CsvBatchWriter(data_dir=DATA_DIR, batch_size=BATCH_SIZE)

    harvester = TickHarvester(
        access_token=cfg["access_token"],
        writer=writer,
        instrument_codes=instrument_codes,
    )

    signal.signal(signal.SIGINT, harvester.request_shutdown)
    signal.signal(signal.SIGTERM, harvester.request_shutdown)

    flusher = PeriodicFlusher(writer=writer, interval=PERIODIC_FLUSH_INTERVAL)
    flusher.start()

    try:
        while not harvester.is_shutdown_requested():
            _sleep_until_market_session()
            if harvester.is_shutdown_requested():
                break
            harvester.run()
    finally:
        flusher.stop()
        writer.flush()
        log.info("Worker shutdown complete.")


if __name__ == "__main__":
    main()
