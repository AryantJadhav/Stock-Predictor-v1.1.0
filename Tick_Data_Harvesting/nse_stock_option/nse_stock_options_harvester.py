#!/usr/bin/env python3
"""
nse_stock_options_harvester.py
==============================
Standalone NSE stock-options harvester (NF CE/PE only).

What this worker does:
- Loads stock underlyings from fo_symbols.txt.
- Builds spot-price anchors from sharekhan.master("NC") for ATM detection.
- Fetches sharekhan.master("NF") and selects stock option contracts only.
- Per stock: keeps current-month expiry only and ATM +/- strike window.
- Streams only NF contracts over WebSocket and writes symbol-wise CSV files.

Zero-pandas architecture: pure Python + csv/json/threading.
"""

from __future__ import annotations

import csv
import json
import logging
import multiprocessing as mp
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
FO_SYMBOLS_FILE_DEFAULT = _BASE_DIR / "fo_symbols.txt"
NSE_HOLIDAYS_FILE = _BASE_DIR / "nse_holidays.txt"

DATA_ROOT_DIR = _BASE_DIR / "tick_data"
LOG_FILE = _BASE_DIR / "nse_stock_options_harvester.log"

EXCHANGE_NSE = "NC"
EXCHANGE_NSE_FO = "NF"

EXCHANGE_TO_WRITER_KEY = {
    EXCHANGE_NSE_FO: "nse_stock_options",
}

# Underlyings that are index symbols; this worker targets stock options only.
INDEX_SYMBOLS: frozenset[str] = frozenset({"NIFTY", "BANKNIFTY", "NIFTYBANK"})

# Contract selection controls.
STRIKE_COUNT_SIDE = 10

BATCH_SIZE = 500
RECONNECT_DELAY_MIN = 5
RECONNECT_DELAY_MAX = 60
PERIODIC_FLUSH_INTERVAL = 60

MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 0
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 45

WS_FEED_CHUNK_SIZE = 500
WS_FEED_CHUNK_DELAY_SEC = 0.35
WS_FEED_HANDSHAKE_PAUSE_SEC = 0.50

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
    format="%(asctime)s  %(levelname)-8s  %(processName)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("nse_stock_options_harvester")


# ============================================================================
# Holiday and trading-day helpers
# ============================================================================
def _load_nse_holidays() -> frozenset[date]:
    holidays: set[date] = set()

    if not NSE_HOLIDAYS_FILE.exists():
        log.warning(
            "nse_holidays.txt not found at %s; weekend-only trading-day guard will be used.",
            NSE_HOLIDAYS_FILE,
        )
        return frozenset()

    try:
        with open(NSE_HOLIDAYS_FILE, "r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                date_part = line.split()[0]
                try:
                    holidays.add(datetime.strptime(date_part, "%d-%m-%Y").date())
                except ValueError:
                    log.warning(
                        "nse_holidays.txt line %d: cannot parse '%s'; skipped.",
                        lineno,
                        date_part,
                    )
    except OSError as exc:
        log.warning("Could not read holiday file %s: %s", NSE_HOLIDAYS_FILE, exc)
        return frozenset()

    log.info("Loaded %d holiday(s) from nse_holidays.txt.", len(holidays))
    return frozenset(holidays)


NSE_HOLIDAYS: frozenset[date] = _load_nse_holidays()


def _is_trading_day(d: date) -> bool:
    return d.weekday() not in (5, 6) and d not in NSE_HOLIDAYS


def _next_trading_day(from_date: date) -> date:
    probe = from_date + timedelta(days=1)
    while not _is_trading_day(probe):
        probe += timedelta(days=1)
    return probe


def _market_open_dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(
        hours=MARKET_OPEN_HOUR,
        minutes=MARKET_OPEN_MINUTE,
    )


def _market_close_dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(
        hours=MARKET_CLOSE_HOUR,
        minutes=MARKET_CLOSE_MINUTE,
    )


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
# Config and symbol loading
# ============================================================================
def _extract_access_token(raw: object) -> str:
    if isinstance(raw, dict):
        for key in ("token", "access_token", "value", "jwtToken", "jwt_token"):
            val = raw.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    if isinstance(raw, str):
        token = raw.strip()
        if token.startswith("{") and token.endswith("}"):
            try:
                parsed = json.loads(token)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                return _extract_access_token(parsed)
        return token

    return ""


def load_config() -> dict:
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


def _build_client_configs(cfg: dict) -> list[dict]:
    """
    Build runtime client configs.

    Supports two modes:
    1) Multi-client mode via config.json key `api_clients` (recommended).
    2) Legacy single-client mode using top-level api_key/access_token.
    """
    clients_raw = cfg.get("api_clients")
    if isinstance(clients_raw, list) and clients_raw:
        clients: list[dict] = []
        for idx, item in enumerate(clients_raw, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"api_clients[{idx}] must be a JSON object.")

            name = str(item.get("name") or f"api{idx}").strip() or f"api{idx}"
            api_key = str(item.get("api_key", "")).strip()
            access_token = _extract_access_token(item.get("access_token", ""))
            symbols_file = str(item.get("symbols_file") or f"fo_symbols_{name}.txt").strip()
            output_subdir = str(item.get("output_subdir") or name).strip() or name

            if not api_key:
                raise ValueError(f"api_clients[{idx}] ({name}): api_key is missing.")
            if not access_token:
                raise ValueError(f"api_clients[{idx}] ({name}): access_token is missing.")

            clients.append(
                {
                    "name": name,
                    "api_key": api_key,
                    "access_token": access_token,
                    "symbols_file": symbols_file,
                    "output_subdir": output_subdir,
                }
            )
        return clients

    # Legacy fallback: run as single client using top-level keys.
    symbols_file = str(cfg.get("symbols_file", FO_SYMBOLS_FILE_DEFAULT.name)).strip() or FO_SYMBOLS_FILE_DEFAULT.name
    output_subdir = str(cfg.get("output_subdir", "legacy")).strip() or "legacy"

    return [
        {
            "name": "legacy",
            "api_key": cfg["api_key"],
            "access_token": cfg["access_token"],
            "symbols_file": symbols_file,
            "output_subdir": output_subdir,
        }
    ]


def _normalise_underlying(symbol: str) -> str:
    upper = symbol.strip().upper()
    if upper == "NIFTYBANK":
        return "BANKNIFTY"
    return upper


def _load_target_symbols(symbols_file: Path) -> set[str]:
    if not symbols_file.exists():
        raise FileNotFoundError(
            f"Symbols file not found at {symbols_file}. Create it with one symbol per line."
        )

    symbols: set[str] = set()
    with open(symbols_file, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            symbol = _normalise_underlying(line)
            if symbol not in INDEX_SYMBOLS:
                symbols.add(symbol)

    if not symbols:
        raise ValueError(
            f"{symbols_file} has no stock symbols. Add stock underlyings and avoid only index names."
        )

    log.info("Target stock underlyings loaded: %d", len(symbols))
    return symbols


# ============================================================================
# Master helpers and contract resolver
# ============================================================================
def _normalise_master_response(raw: object, exchange: str) -> list[dict]:
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


def _parse_expiry_date(raw: str) -> date | None:
    value = raw.strip()
    for sep in ("T", " "):
        if sep in value:
            value = value.split(sep)[0]

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%b%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _extract_price(record: dict) -> float | None:
    for price_field in (
        "close",
        "Close",
        "closePrice",
        "prevClose",
        "PrevClose",
        "lastClose",
        "last_close",
        "ltp",
        "Ltp",
        "LTP",
        "lastTradedPrice",
    ):
        raw_price = record.get(price_field)
        if raw_price is None or str(raw_price).strip() == "":
            continue
        try:
            value = float(raw_price)
            if value > 0:
                return value
        except (ValueError, TypeError):
            continue
    return None


def _format_strike(strike: float) -> str:
    return str(int(strike)) if float(strike).is_integer() else f"{strike:g}"


def _select_stock_monthly_expiries(sorted_expiries: list[date], today: date) -> list[date]:
    # Current-month-only rule: keep only the nearest expiry in this month.
    current_month = [
        d for d in sorted_expiries
        if d >= today and d.year == today.year and d.month == today.month
    ]
    if not current_month:
        return []
    return [current_month[0]]


def build_spot_prices_from_nc(
    sharekhan: SharekhanConnect,
    target_symbols: set[str],
) -> dict[str, float]:
    try:
        raw_nc = sharekhan.master(EXCHANGE_NSE)
    except Exception as exc:
        raise RuntimeError(f"sharekhan.master('NC') failed: {exc}") from exc

    records = _normalise_master_response(raw_nc, EXCHANGE_NSE)
    log.info("NC master rows received: %d", len(records))

    spot_prices: dict[str, float] = {}

    for record in records:
        sym_raw = _get_field(
            record,
            "TradingSymbol",
            "tradingSymbol",
            "tradingsymbol",
            "TRADINGSYMBOL",
            "Symbol",
            "symbol",
            "Name",
            "name",
        )
        if not sym_raw:
            continue

        symbol = _normalise_underlying(sym_raw)
        if symbol not in target_symbols:
            continue

        price = _extract_price(record)
        if price is not None:
            spot_prices[symbol] = price

    log.info(
        "Spot anchors built from NC master: %d/%d symbol(s).",
        len(spot_prices),
        len(target_symbols),
    )

    return spot_prices


def fetch_nf_stock_option_codes(
    sharekhan: SharekhanConnect,
    target_symbols: set[str],
    spot_prices: dict[str, float],
) -> list[str]:
    """
    Resolve NF stock-option (CE/PE) subscription codes.

    Rules:
    - Underlyings: symbols loaded from fo_symbols.txt (stock names only).
    - Expiries: current-month expiry only.
    - Strikes: ATM +/- STRIKE_COUNT_SIDE from sorted strikes per expiry.
    - If spot anchor is unavailable, includes all strikes in selected expiries.
    """
    log.info("Fetching NSE F&O master (exchange=NF) for %d stock(s)...", len(target_symbols))
    try:
        raw_nf = sharekhan.master(EXCHANGE_NSE_FO)
    except Exception as exc:
        raise RuntimeError(f"sharekhan.master('NF') failed: {exc}") from exc

    records = _normalise_master_response(raw_nf, EXCHANGE_NSE_FO)
    log.info("NF master rows received: %d", len(records))

    today = date.today()

    # underlying -> expiry -> opt_type -> list[(strike, code, label)]
    candidates: dict[str, dict[date, dict[str, list[tuple[float, str, str]]]]] = {
        sym: {} for sym in target_symbols
    }

    for record in records:
        opt_type = _get_field(record, "optionType", "OptionType", "option_type").upper()
        if opt_type not in ("CE", "PE"):
            continue

        underlying_raw = _get_field(
            record,
            "tradingSymbol",
            "TradingSymbol",
            "tradingsymbol",
            "Symbol",
            "symbol",
            "name",
            "Name",
        )
        if not underlying_raw:
            continue

        underlying = _normalise_underlying(underlying_raw)
        if underlying not in target_symbols:
            continue

        expiry_raw = _get_field(
            record,
            "expiry",
            "ExpiryDate",
            "expiryDate",
            "Expiry",
            "ExpiryDateTime",
            "MaturityDate",
            "expiry_date",
        )
        if not expiry_raw or expiry_raw.strip() in ("", "0"):
            continue

        expiry_dt = _parse_expiry_date(expiry_raw)
        if expiry_dt is None or expiry_dt < today:
            continue

        code = _get_field(
            record,
            "scripCode",
            "ScripCode",
            "scripcode",
            "SCRIPCODE",
            "Code",
            "code",
            "scripId",
            "ScripId",
        )
        if not code:
            continue

        try:
            strike = float(record.get("strike") or record.get("Strike") or 0)
        except (ValueError, TypeError):
            strike = 0.0
        if strike <= 0:
            continue

        full_label = _get_field(
            record,
            "fullName",
            "FullName",
            "symbolName",
            "SymbolName",
            "scripName",
            "ScripName",
        )
        if not full_label:
            exp_compact = expiry_dt.strftime("%d%b%y").upper()
            full_label = f"{underlying}{exp_compact}{_format_strike(strike)}{opt_type}"

        by_expiry = candidates[underlying].setdefault(expiry_dt, {"CE": [], "PE": []})
        by_expiry[opt_type].append((strike, code, full_label))

    selected_codes: list[str] = []
    seen: set[str] = set()

    def _add_nf(code: str, label: str) -> None:
        instrument = f"{EXCHANGE_NSE_FO}{code}"
        if instrument in seen:
            return
        seen.add(instrument)
        selected_codes.append(instrument)
        CODE_TO_SYMBOL[instrument] = label

    total_options = 0

    for underlying in sorted(target_symbols):
        by_expiry = candidates.get(underlying, {})
        if not by_expiry:
            log.warning("%s: no stock option contracts found in NF master.", underlying)
            continue

        selected_expiries = _select_stock_monthly_expiries(sorted(by_expiry.keys()), today)
        if not selected_expiries:
            log.warning("%s: no eligible expiries found.", underlying)
            continue

        spot = spot_prices.get(underlying, 0.0)
        use_spot_anchor = spot > 0

        count_before = len(selected_codes)

        for expiry_dt in selected_expiries:
            rows = by_expiry.get(expiry_dt, {})
            strike_values = sorted(
                {strike for strike, _, _ in rows.get("CE", [])}
                | {strike for strike, _, _ in rows.get("PE", [])}
            )
            if not strike_values:
                continue

            # Always apply 21-strike filter (ATM ± 10)
            if use_spot_anchor:
                atm_strike = min(strike_values, key=lambda x: abs(x - spot))
            else:
                # Fallback: use median strike when spot anchor is missing
                atm_strike = strike_values[len(strike_values) // 2]
                log.warning(
                    "%s: missing NC spot anchor; using median strike (%.2f) as ATM fallback.",
                    underlying,
                    atm_strike,
                )

            atm_idx = strike_values.index(atm_strike)
            start_idx = max(0, atm_idx - STRIKE_COUNT_SIDE)
            end_idx = min(len(strike_values), atm_idx + STRIKE_COUNT_SIDE + 1)
            target_strikes = set(strike_values[start_idx:end_idx])

            for opt_type in ("CE", "PE"):
                for strike, code, label in rows.get(opt_type, []):
                    if strike not in target_strikes:
                        continue
                    _add_nf(code, label)

            low = min(target_strikes)
            high = max(target_strikes)
            anchor_type = "spot_anchor" if use_spot_anchor else "median_fallback"
            log.info(
                "%s %s: %s=%.2f atm=%.2f strike_window=[%.2f..%.2f] strikes=%d",
                underlying,
                expiry_dt.isoformat(),
                anchor_type,
                spot if use_spot_anchor else strike_values[len(strike_values) // 2],
                atm_strike,
                low,
                high,
                len(target_strikes),
            )

        added = len(selected_codes) - count_before
        total_options += added
        log.info(
            "%s stock options: expiries=%s contracts=%d",
            underlying,
            ",".join(d.isoformat() for d in selected_expiries),
            added,
        )

    log.info("NF stock-option selection complete: total_contracts=%d", total_options)

    if not selected_codes:
        raise RuntimeError("NF stock-option selection returned 0 instruments.")

    return selected_codes


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
# Harvester engine
# ============================================================================
class TickHarvester:
    def __init__(self, access_token: str, writer: CsvBatchWriter, instrument_codes: list[str], client_name: str) -> None:
        self._access_token = access_token
        self._writer = writer
        self._instrument_codes = instrument_codes
        self._client_name = client_name
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
            log.info("Market close reached (15:45). Closing socket and sleeping until next session.")
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
        log.info("[%s] WebSocket connected. Subscribing to %d NF instrument(s)...", self._client_name, len(self._instrument_codes))
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
                log.info("[%s] Feed subscription progress: %d/%d chunk(s).", self._client_name, idx, len(chunks))
            if idx < len(chunks):
                time.sleep(WS_FEED_CHUNK_DELAY_SEC)

        log.info(
            "[%s] Live feed subscription sent for %d NF instrument(s) in %d chunk(s).",
            self._client_name,
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
            if writer_key != "nse_stock_options":
                continue
            self._writer.add(tick)

    def _on_error(self, _wsapp: object, error: object) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, *_args: object) -> None:
        log.warning("WebSocket closed by server or network.")

    def run(self) -> None:
        log.info(
            "[%s] NSE Stock Options worker starting. instruments=%d batch=%d",
            self._client_name,
            len(self._instrument_codes),
            BATCH_SIZE,
        )

        attempt = 0
        while not self._shutdown.is_set():
            if not _is_market_session_now():
                log.info("[%s] Outside market session window (09:00-15:45). Returning to scheduler.", self._client_name)
                break

            attempt += 1
            log.info("[%s] Connection attempt #%d (next_backoff=%ds)", self._client_name, attempt, self._reconnect_delay)

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
                log.info("[%s] Market session ended. Returning to scheduler for overnight sleep.", self._client_name)
                break

            log.info("[%s] Disconnected. Retrying in %ds...", self._client_name, self._reconnect_delay)
            for _ in range(self._reconnect_delay):
                if self._shutdown.is_set():
                    break
                time.sleep(1)

            self._reconnect_delay = min(self._reconnect_delay * 2, RECONNECT_DELAY_MAX)

        log.info("Reconnect loop exited after %d attempt(s).", attempt)


def _run_single_client(client_cfg: dict) -> None:
    client_name = str(client_cfg.get("name", "client"))
    symbols_file_name = str(client_cfg["symbols_file"])
    symbols_file_path = _BASE_DIR / symbols_file_name
    data_dir = DATA_ROOT_DIR / str(client_cfg["output_subdir"])

    log.info("[%s] Worker boot. symbols_file=%s output_dir=%s", client_name, symbols_file_path, data_dir)

    CODE_TO_SYMBOL.clear()

    rest_client = SharekhanConnect(client_cfg["api_key"], client_cfg["access_token"])

    try:
        target_symbols = _load_target_symbols(symbols_file_path)
        spot_prices = build_spot_prices_from_nc(rest_client, target_symbols)
        instrument_codes = fetch_nf_stock_option_codes(
            sharekhan=rest_client,
            target_symbols=target_symbols,
            spot_prices=spot_prices,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        log.critical("[%s] NF stock-option instrument resolution failed: %s", client_name, exc)
        raise SystemExit(1) from exc

    if any(not code.startswith(EXCHANGE_NSE_FO) for code in instrument_codes):
        raise SystemExit(f"[{client_name}] Invalid subscription list: non-NF code detected.")

    INSTRUMENT_CODES[:] = instrument_codes

    log.info(
        "[%s] Total NF stock-option instruments: %d. First 10: %s%s",
        client_name,
        len(instrument_codes),
        ", ".join(instrument_codes[:10]),
        " ..." if len(instrument_codes) > 10 else "",
    )

    writer = CsvBatchWriter(data_dir=data_dir, batch_size=BATCH_SIZE)

    harvester = TickHarvester(
        access_token=client_cfg["access_token"],
        writer=writer,
        instrument_codes=instrument_codes,
        client_name=client_name,
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
        log.info("[%s] Worker shutdown complete.", client_name)


def _worker_entry(client_cfg: dict) -> None:
    try:
        _run_single_client(client_cfg)
    except KeyboardInterrupt:
        pass


def _start_worker_process(ctx: mp.context.BaseContext, client_cfg: dict) -> mp.Process:
    name = str(client_cfg.get("name", "client"))
    proc = ctx.Process(target=_worker_entry, args=(client_cfg,), name=f"worker-{name}")
    proc.start()
    log.info("Supervisor started worker '%s' (pid=%s).", name, proc.pid)
    return proc


# ============================================================================
# Entry point
# ============================================================================
def main() -> None:
    log.info("=" * 60)
    log.info("NSE Stock Options Harvester starting (multi-client capable)")
    log.info("=" * 60)

    try:
        cfg = load_config()
    except ValueError as exc:
        log.critical("Configuration error: %s", exc)
        raise SystemExit(1) from exc

    try:
        clients = _build_client_configs(cfg)
    except ValueError as exc:
        log.critical("Client configuration error: %s", exc)
        raise SystemExit(1) from exc

    log.info("Configured client workers: %d", len(clients))
    for client in clients:
        log.info(
            "Client=%s symbols_file=%s output_subdir=%s",
            client["name"],
            client["symbols_file"],
            client["output_subdir"],
        )

    if len(clients) == 1:
        _run_single_client(clients[0])
        return

    ctx = mp.get_context("spawn")
    workers: dict[str, mp.Process] = {}
    restart_delay: dict[str, int] = {}
    max_restart_delay = 120

    for client in clients:
        name = str(client["name"])
        workers[name] = _start_worker_process(ctx, client)
        restart_delay[name] = RECONNECT_DELAY_MIN

    try:
        while True:
            time.sleep(2)
            for client in clients:
                name = str(client["name"])
                proc = workers[name]
                if proc.is_alive():
                    continue

                code = proc.exitcode
                delay = restart_delay[name]
                log.error(
                    "Worker '%s' exited with code %s. Restarting in %ds.",
                    name,
                    code,
                    delay,
                )
                time.sleep(delay)
                workers[name] = _start_worker_process(ctx, client)
                restart_delay[name] = min(delay * 2, max_restart_delay)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt in supervisor. Stopping all workers...")
    finally:
        for name, proc in workers.items():
            if proc.is_alive():
                log.info("Terminating worker '%s' (pid=%s).", name, proc.pid)
                proc.terminate()
        for name, proc in workers.items():
            proc.join(timeout=10)
            if proc.is_alive():
                log.warning("Worker '%s' did not stop in time.", name)
        log.info("Supervisor shutdown complete.")


if __name__ == "__main__":
    main()
