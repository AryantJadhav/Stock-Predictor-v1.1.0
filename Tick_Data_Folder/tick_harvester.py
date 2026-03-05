#!/usr/bin/env python3
"""
tick_harvester.py
=================
Live tick-data harvester for NSE/BSE F&O underlying spot prices +
NIFTY 50, NIFTY BANK, and SENSEX indices, using the official
Sharekhan shareconnect Python SDK.

Target hardware : AWS EC2 t3.small  (2 GB RAM) – Ubuntu LTS
Design goals    :
  ✔  Dynamic scrip-code resolution at boot via sharekhan.master()
  ✔  Zero pandas / zero SQLite  →  csv module only
  ✔  RAM-safe 500-tick batch flush
  ✔  Daily CSV file rollover at midnight
  ✔  Exponential-backoff auto-reconnect
  ✔  Graceful shutdown on SIGINT / SIGTERM  (flushes RAM buffer to disk)

Quick-start:
  1.  Run auth_helper.py once to obtain your access_token → config.json
  2.  Populate fo_symbols.txt (one symbol per line, e.g. RELIANCE, NIFTY)
  3.  python tick_harvester.py          ← scrip codes resolved automatically
"""

# ─────────────────────────────────────────────────────────────────────────────
#  Standard-library imports  (no external RAM hogs)
# ─────────────────────────────────────────────────────────────────────────────
import csv
import json
import logging
import os
import signal
import threading
import time
from datetime import date, datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
#  Sharekhan SDK import
#  Install: pip install shareconnect websocket-client
# ─────────────────────────────────────────────────────────────────────────────
from SharekhanApi.sharekhanConnect  import SharekhanConnect    # type: ignore  (REST client)
from SharekhanApi.sharekhanWebsocket import SharekhanWebSocket  # type: ignore  (streaming)


# ═════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════

# Paths (relative to this script's directory so the EC2 user doesn't need to
# care about the current working directory when running the script)
_BASE_DIR   = Path(__file__).parent
CONFIG_FILE = _BASE_DIR / "config.json"    # your API credentials
DATA_DIR    = _BASE_DIR / "tick_data"      # all daily CSV files land here
LOG_FILE        = _BASE_DIR / "harvester.log"    # rotated by the OS or manually
FO_SYMBOLS_FILE = _BASE_DIR / "fo_symbols.txt"   # one symbol per line (e.g. RELIANCE)

# Exchange prefixes used when building WebSocket instrument codes
# NC  – NSE Cash / Index  (spot prices for F&O stocks, NIFTY 50, NIFTY BANK)
# BC  – BSE Cash / Index  (SENSEX)
EXCHANGE_NSE = "NC"
EXCHANGE_BSE = "BC"

# Symbols that live on BSE rather than NSE — only SENSEX currently
# (all other F&O underlyings are on NSE).
BSE_ONLY_SYMBOLS: frozenset[str] = frozenset({"SENSEX"})

# CSV flush threshold – at 500 items the buffer is written to disk and cleared
BATCH_SIZE = 500

# Reconnect back-off (seconds)
RECONNECT_DELAY_MIN = 5    # first retry after 5 s
RECONNECT_DELAY_MAX = 60   # cap the back-off at 60 s

# Periodic safety flush – even if the buffer never hits 500 items, flush every
# N seconds so we don't lose data during quiet market periods
PERIODIC_FLUSH_INTERVAL = 60  # seconds


# ─────────────────────────────────────────────────────────────────────────────
#  INSTRUMENT CODES  (populated dynamically at boot by fetch_dynamic_scrip_codes)
#
#  Format after population: {ExchangePrefix}{scripCode}
#    NC{n}  – NSE Cash spot price   e.g.  NC22  (Infosys)
#    BC{n}  – BSE Cash spot price   e.g.  BC1   (SENSEX)
#
#  This list is intentionally empty here; main() calls
#  fetch_dynamic_scrip_codes() to resolve live scrip codes from the
#  Sharekhan master list and assign the result to this variable.
# ─────────────────────────────────────────────────────────────────────────────
INSTRUMENT_CODES: list[str] = []  # filled at boot — see fetch_dynamic_scrip_codes()

# CSV header row – matches the flat dict produced by parse_tick()
CSV_HEADERS: list[str] = [
    "received_at",     # ISO-8601 local timestamp with milliseconds
    "symbol",          # tradingSymbol  e.g. NIFTY23MAR22000CE
    "exchange",        # e.g. NF
    "ltp",             # last traded price
    "open",
    "high",
    "low",
    "close",
    "volume",          # total traded quantity for the day
    "oi",              # open interest (critical for F&O)
    "raw",             # full raw JSON – safety net if the schema changes
]


# ═════════════════════════════════════════════════════════════════════════════
#  LOGGING  (dual output: file + stdout so systemd's journald captures it too)
# ═════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),   # stdout → systemd journal / screen
    ],
)
log = logging.getLogger("tick_harvester")


# ═════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION LOADER
# ═════════════════════════════════════════════════════════════════════════════
def load_config() -> dict:
    """
    Load credentials from config.json.

    Environment variables take precedence over the JSON file.
    This lets you use AWS Secrets Manager / Parameter Store / Docker secrets
    without touching the file on disk.

    Required keys:
        api_key       → SHAREKHAN_API_KEY
        access_token  → SHAREKHAN_ACCESS_TOKEN
        secret_key    → SHAREKHAN_SECRET_KEY   (stored for reference only)
        customer_id   → SHAREKHAN_CUSTOMER_ID  (stored for reference only)
    """
    cfg: dict = {}

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
                cfg = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Could not read config.json: %s — falling back to env vars", exc)

    # Environment-variable overrides
    cfg["api_key"]      = os.getenv("SHAREKHAN_API_KEY",      cfg.get("api_key",      ""))
    cfg["secret_key"]   = os.getenv("SHAREKHAN_SECRET_KEY",   cfg.get("secret_key",   ""))
    cfg["customer_id"]  = os.getenv("SHAREKHAN_CUSTOMER_ID",  cfg.get("customer_id",  ""))
    cfg["access_token"] = os.getenv("SHAREKHAN_ACCESS_TOKEN", cfg.get("access_token", ""))

    if not cfg.get("api_key"):
        raise ValueError("api_key is missing. Fill config.json or set SHAREKHAN_API_KEY.")
    if not cfg.get("access_token"):
        raise ValueError(
            "access_token is missing. "
            "Run 'python auth_helper.py' to obtain one and save it to config.json."
        )

    return cfg


# ═════════════════════════════════════════════════════════════════════════════
#  DYNAMIC SCRIP-CODE FETCHER
# ═════════════════════════════════════════════════════════════════════════════

def _load_fo_symbols(symbols_file: Path) -> set[str]:
    """
    Read fo_symbols.txt and return a set of normalised symbol names.

    Rules applied to each line:
      •  Strip leading/trailing whitespace
      •  Skip blank lines and comment lines that start with '#'
      •  Uppercase everything for case-insensitive matching
    """
    if not symbols_file.exists():
        raise FileNotFoundError(
            f"fo_symbols.txt not found at {symbols_file}\n"
            "Create it with one symbol per line, e.g.:\n"
            "  RELIANCE\n  HDFCBANK\n  NIFTY\n  BANKNIFTY\n  SENSEX"
        )

    symbols: set[str] = set()
    with open(symbols_file, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            symbols.add(line.upper())

    if not symbols:
        raise ValueError(
            f"{symbols_file} is empty or contains only comments. "
            "Add at least one symbol (e.g. RELIANCE) and restart."
        )

    log.info("fo_symbols.txt  →  %d target symbol(s) loaded.", len(symbols))
    return symbols


def _normalise_master_response(raw: object, exchange: str) -> list[dict]:
    """
    Defensive normaliser for sharekhan.master() return values.

    The Sharekhan REST API may return:
      (a)  a Python list of dicts                               → use as-is
      (b)  a dict with a data/result/records wrapper key        → unwrap
      (c)  a JSON string (some SDK versions return raw strings) → decode first
      (d)  None or an error dict                                → return []
    """
    # Handle JSON string responses from older SDK builds
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.error("master(%s): JSON decode failed — %s", exchange, exc)
            return []

    if raw is None:
        log.error("master(%s) returned None.", exchange)
        return []

    if isinstance(raw, list):
        return raw  # most common case

    if isinstance(raw, dict):
        # Try common wrapper keys used by Indian broker REST APIs
        for key in ("data", "result", "records", "scriptMaster", "ScriptMaster"):
            if isinstance(raw.get(key), list):
                log.debug("master(%s): unwrapped key '%s'", exchange, key)
                return raw[key]
        log.error(
            "master(%s): unexpected dict shape — keys=%s", exchange, list(raw.keys())[:10]
        )
        return []

    log.error("master(%s): unexpected type %s", exchange, type(raw).__name__)
    return []


def _get_field(record: dict, *candidate_keys: str) -> str:
    """
    Return the first non-empty string value found among candidate_keys.
    Returns "" if none match.  All comparisons are key-exact (no case-fold
    here — the caller normalises to upper after calling this function).
    """
    for key in candidate_keys:
        val = record.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def fetch_dynamic_scrip_codes(
    sharekhan: SharekhanConnect,
    symbols_file: Path = FO_SYMBOLS_FILE,
) -> list[str]:
    """
    Boot-time scrip-code resolver.  Runs ONCE at startup.

    Algorithm
    ---------
    1.  Load fo_symbols.txt → a set of target symbol names (uppercased).
    2.  Call sharekhan.master("NC") to get the full NSE Cash/Index master.
        Match each row whose tradingSymbol appears in target_symbols.
        → These cover all F&O underlying equities + NIFTY 50 + NIFTY BANK.
    3.  Call sharekhan.master("BC") to get the full BSE Cash/Index master.
        Only match rows in BSE_ONLY_SYMBOLS (currently just SENSEX) to
        avoid creating duplicate codes for stocks that trade on both exchanges.
    4.  Build the instrument code strings as "{ExchangePrefix}{scripCode}"
        (e.g. "NC500325" for RELIANCE-NSE, "BC1" for SENSEX).
    5.  Warn about any requested symbol that was not found in either master.
    6.  Return the final deduplicated list.

    Zero-pandas design
    ------------------
    The master lists can contain 10 000+ rows.  We iterate them with
    pure-Python list comprehensions — O(n) time, O(k) memory where k is
    the number of matched symbols (at most ~229 entries).

    Field-name resilience
    ---------------------
    The Sharekhan master schema is not publicly documented.  We probe
    multiple common PascalCase / camelCase / UPPERCASE key variants for
    both the scrip code and the trading symbol columns.
    """
    # ── Step 0: Load target symbols from disk ────────────────────────────
    target_symbols: set[str] = _load_fo_symbols(symbols_file)

    # Symbols to look up on NSE (everything that is NOT BSE-only)
    nse_targets: set[str] = target_symbols - BSE_ONLY_SYMBOLS
    # Symbols to look up on BSE  (only BSE_ONLY_SYMBOLS that the user listed)
    bse_targets: set[str] = target_symbols & BSE_ONLY_SYMBOLS

    instrument_codes: list[str] = []
    matched_symbols:  set[str]  = set()   # track which ones we resolved

    # ── Step 1: NSE Cash master  ("NC") ─────────────────────────────────
    if nse_targets:
        log.info("Fetching NSE Cash master  (exchange=NC) …")
        try:
            raw_nc = sharekhan.master(EXCHANGE_NSE)
        except Exception as exc:
            raise RuntimeError(f"sharekhan.master('NC') failed: {exc}") from exc

        nc_records: list[dict] = _normalise_master_response(raw_nc, EXCHANGE_NSE)
        log.info("  NC master rows received : %d", len(nc_records))

        for record in nc_records:
            # Try all known field-name variants for the trading symbol
            sym_raw = _get_field(
                record,
                "TradingSymbol", "tradingSymbol", "tradingsymbol",
                "TRADINGSYMBOL", "Symbol", "symbol", "Name", "name",
            )
            if not sym_raw:
                continue

            sym = sym_raw.upper()
            if sym not in nse_targets:
                continue  # not on our watch-list

            # Try all known field-name variants for the numeric scrip code
            code = _get_field(
                record,
                "ScripCode", "scripCode", "scripcode", "SCRIPCODE",
                "Scripcode", "Code", "code", "scripId", "ScripId",
            )
            if not code:
                log.warning("NC: found symbol '%s' but no scrip code in record — skipping.", sym)
                continue

            instrument_codes.append(f"{EXCHANGE_NSE}{code}")
            matched_symbols.add(sym)

        log.info(
            "  NC matches : %d / %d  →  %s",
            len(matched_symbols & nse_targets),
            len(nse_targets),
            ", ".join(sorted(matched_symbols & nse_targets)) or "none",
        )

    # ── Step 2: BSE Cash master  ("BC") ──────────────────────────────────
    if bse_targets:
        log.info("Fetching BSE Cash master  (exchange=BC) …")
        try:
            raw_bc = sharekhan.master(EXCHANGE_BSE)
        except Exception as exc:
            raise RuntimeError(f"sharekhan.master('BC') failed: {exc}") from exc

        bc_records: list[dict] = _normalise_master_response(raw_bc, EXCHANGE_BSE)
        log.info("  BC master rows received : %d", len(bc_records))

        bc_matched: set[str] = set()
        for record in bc_records:
            sym_raw = _get_field(
                record,
                "TradingSymbol", "tradingSymbol", "tradingsymbol",
                "TRADINGSYMBOL", "Symbol", "symbol", "Name", "name",
            )
            if not sym_raw:
                continue

            sym = sym_raw.upper()
            if sym not in bse_targets:
                continue

            code = _get_field(
                record,
                "ScripCode", "scripCode", "scripcode", "SCRIPCODE",
                "Scripcode", "Code", "code", "scripId", "ScripId",
            )
            if not code:
                log.warning("BC: found symbol '%s' but no scrip code — skipping.", sym)
                continue

            instrument_codes.append(f"{EXCHANGE_BSE}{code}")
            bc_matched.add(sym)
            matched_symbols.add(sym)

        log.info(
            "  BC matches : %d / %d  →  %s",
            len(bc_matched),
            len(bse_targets),
            ", ".join(sorted(bc_matched)) or "none",
        )

    # ── Step 3: Warn about unresolved symbols ─────────────────────────────
    unresolved: set[str] = target_symbols - matched_symbols
    if unresolved:
        log.warning(
            "%d symbol(s) in fo_symbols.txt could NOT be matched in any master list:\n"
            "  %s\n"
            "  Check spelling vs. the Sharekhan master  "
            "(run auth_helper.py and call sharekhan.master('NC') interactively).",
            len(unresolved),
            ", ".join(sorted(unresolved)),
        )

    # ── Step 4: Deduplicate while preserving insertion order ─────────────
    seen: set[str] = set()
    unique_codes: list[str] = []
    for code in instrument_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)

    if not unique_codes:
        raise RuntimeError(
            "fetch_dynamic_scrip_codes() resolved 0 instruments.\n"
            "Check fo_symbols.txt contents and your Sharekhan API access."
        )

    log.info(
        "Dynamic scrip resolution complete  →  %d instrument code(s) ready.",
        len(unique_codes),
    )
    return unique_codes


# ═════════════════════════════════════════════════════════════════════════════
#  RAM-SAFE CSV BATCH WRITER
# ═════════════════════════════════════════════════════════════════════════════
class CsvBatchWriter:
    """
    Thread-safe CSV writer with zero-pandas, zero-SQLite design.

    Mechanics
    ---------
    •  Incoming tick dicts accumulate in self._buffer (a plain Python list).
    •  The moment the list reaches BATCH_SIZE entries the buffer is appended
       to the day's CSV file and immediately clear()d — freeing RAM.
    •  At midnight the output file path changes automatically (daily rollover).
    •  flush() can be called at any time to force-write whatever is in the
       buffer (used on shutdown and by the periodic safety timer).
    •  All public methods are protected by a threading.Lock so the writer is
       safe to call from multiple threads.
    """

    def __init__(self, data_dir: Path, batch_size: int = BATCH_SIZE) -> None:
        self._dir        = data_dir
        self._batch_size = batch_size
        self._buffer: list[dict]  = []
        self._lock         = threading.Lock()
        self._current_date = date.today()
        self._dir.mkdir(parents=True, exist_ok=True)
        log.info("CsvBatchWriter ready — writing to %s (batch=%d)", self._dir, batch_size)

    # ──────────────────────────────────────────────────────────────────────
    def _csv_path(self) -> Path:
        """
        Return the path for today's CSV file.
        Detects a date change (midnight rollover) and logs it.
        """
        today = date.today()
        if today != self._current_date:
            log.info(
                "Midnight rollover: switching from ticks_%s.csv → ticks_%s.csv",
                self._current_date.isoformat(),
                today.isoformat(),
            )
            self._current_date = today
        return self._dir / f"ticks_{self._current_date.isoformat()}.csv"

    # ──────────────────────────────────────────────────────────────────────
    def _flush_locked(self) -> None:
        """
        Write the buffer to disk and clear it.
        MUST be called while self._lock is already held.
        """
        if not self._buffer:
            return  # nothing to do

        target = self._csv_path()
        file_existed = target.exists()
        n = len(self._buffer)

        try:
            with open(target, "a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=CSV_HEADERS,
                    extrasaction="ignore",  # ignore unknown keys from API
                )
                if not file_existed:
                    # Write the header row only for brand-new files
                    writer.writeheader()
                writer.writerows(self._buffer)

            # ← This is the critical line: free RAM immediately after disk write
            self._buffer.clear()

            log.debug("Flushed %d ticks → %s", n, target.name)

        except OSError as exc:
            # Do NOT clear the buffer on failure — retry on the next flush
            log.error("Disk write failed (%s). Buffer retained (%d items).", exc, n)

    # ──────────────────────────────────────────────────────────────────────
    def add(self, tick: dict) -> None:
        """
        Add one tick to the in-memory buffer.
        Triggers an automatic flush when buffer reaches batch_size.
        """
        with self._lock:
            self._buffer.append(tick)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()

    # ──────────────────────────────────────────────────────────────────────
    def flush(self) -> None:
        """
        Force-flush the buffer to disk regardless of its current size.
        Called by the periodic safety timer and on shutdown.
        """
        with self._lock:
            count = len(self._buffer)
            self._flush_locked()
            if count:
                log.info("Safety flush: wrote %d buffered ticks to disk.", count)

    # ──────────────────────────────────────────────────────────────────────
    def __len__(self) -> int:
        """Return number of ticks currently held in RAM."""
        with self._lock:
            return len(self._buffer)


# ═════════════════════════════════════════════════════════════════════════════
#  TICK PARSER
# ═════════════════════════════════════════════════════════════════════════════
def parse_tick(raw_message: object) -> dict | None:
    """
    Convert a raw WebSocket message from Sharekhan into a flat dict
    that maps directly to CSV_HEADERS.

    The Sharekhan API response is undocumented at the field level; the
    key names below are inferred from SDK examples and live observations.
    If a field is absent the cell is left empty ("") — never raises.

    All parsing errors are logged as warnings and return None so the
    harvester keeps running.

    NOTE: The 'raw' column stores the entire JSON payload.  If the API
          schema changes between contract expiries or SDK updates, your
          raw column guarantees you always have the source data.
    """
    try:
        # The SDK routes text frames here as raw strings; binary frames
        # go through _parse_binary_data (stub in SDK → returns None)
        if raw_message is None:
            return None

        if raw_message == "pong":
            # Heartbeat reply – not a tick
            return None

        data: dict = (
            json.loads(raw_message)
            if isinstance(raw_message, str)
            else raw_message
        )

        if not isinstance(data, dict):
            # Could be a list wrapper or error envelope – log and skip
            log.debug("Non-dict tick skipped: %s", str(raw_message)[:120])
            return None

        tick: dict = {
            "received_at": datetime.now().isoformat(timespec="milliseconds"),

            # Symbol / identification
            #   Sharekhan may return either "tradingSymbol" or "symbol"
            "symbol":   data.get("tradingSymbol") or data.get("symbol",   ""),
            "exchange": data.get("exchange",                               ""),

            # Price fields
            "ltp":   data.get("ltp")    or data.get("lastTradedPrice",    ""),
            "open":  data.get("open",                                      ""),
            "high":  data.get("high",                                      ""),
            "low":   data.get("low",                                       ""),
            "close": data.get("close",                                     ""),

            # Volume / interest
            "volume": data.get("volume") or data.get("totalTradeQty",     ""),
            "oi":     data.get("oi")     or data.get("openInterest",       ""),

            # Full raw payload as a fallback / audit trail
            "raw": json.dumps(data, separators=(",", ":")),
        }

        return tick

    except (json.JSONDecodeError, AttributeError, TypeError) as exc:
        log.warning("Tick parse error: %s | raw=%s", exc, str(raw_message)[:200])
        return None


# ═════════════════════════════════════════════════════════════════════════════
#  PERIODIC SAFETY FLUSH  (background daemon thread)
# ═════════════════════════════════════════════════════════════════════════════
class PeriodicFlusher(threading.Thread):
    """
    A daemon thread that calls CsvBatchWriter.flush() every
    PERIODIC_FLUSH_INTERVAL seconds.

    Rationale: During pre-market or post-market hours tick volume is low.
    Without this, a nearly-full buffer could sit in RAM for a long time.
    """

    def __init__(self, writer: CsvBatchWriter, interval: int = PERIODIC_FLUSH_INTERVAL) -> None:
        super().__init__(name="periodic-flusher", daemon=True)
        self._writer   = writer
        self._interval = interval
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        log.info("PeriodicFlusher started (interval=%ds).", self._interval)
        while not self._stop_evt.wait(timeout=self._interval):
            self._writer.flush()
        log.info("PeriodicFlusher stopped.")


# ═════════════════════════════════════════════════════════════════════════════
#  TICK HARVESTER  (WebSocket lifecycle manager)
# ═════════════════════════════════════════════════════════════════════════════
class TickHarvester:
    """
    Owns the Sharekhan WebSocket connection and the reconnect loop.

    Life cycle
    ----------
    1.  run() starts an outer while-loop that attempts to connect.
    2.  connect() is BLOCKING — it calls sws.connect() which internally
        calls websocket.WebSocketApp.run_forever().
    3.  When the socket drops (server close, network error, etc.) connect()
        returns and the loop waits `reconnect_delay` seconds then retries.
    4.  The delay doubles on every failed attempt (exponential back-off)
        capped at RECONNECT_DELAY_MAX.
    5.  On the first successful open(), the delay resets to the minimum.
    6.  request_shutdown() (triggered by SIGINT/SIGTERM) sets the stop
        event, closes the socket, and calls writer.flush() before exiting.
    """

    def __init__(
        self,
        access_token:     str,
        writer:           CsvBatchWriter,
        instrument_codes: list[str],
    ) -> None:
        self._access_token     = access_token
        self._writer           = writer
        self._instrument_codes = instrument_codes   # resolved at boot
        self._sws: SharekhanWebSocket | None = None
        self._shutdown         = threading.Event()
        self._reconnect_delay  = RECONNECT_DELAY_MIN

    # ── Signal / shutdown ─────────────────────────────────────────────────
    def request_shutdown(self, signum: int = 0, _frame: object = None) -> None:
        """
        Called by the OS signal handlers (SIGINT / SIGTERM).
        Gracefully flushes RAM buffer before the process dies.
        """
        sig_name = signal.Signals(signum).name if signum else "programmatic"
        log.info("Shutdown requested (%s). Flushing %d buffered ticks…", sig_name, len(self._writer))
        self._shutdown.set()

        # Tell the running WebSocket to close cleanly
        if self._sws is not None:
            try:
                self._sws.close_connection()
            except Exception as exc:
                log.debug("close_connection() raised (harmless): %s", exc)

        # Flush whatever is left in RAM to disk
        self._writer.flush()
        log.info("Buffer flushed. Shutting down.")

    # ── WebSocket callbacks ──────────────────────────────────────────────
    def _on_open(self, wsapp: object) -> None:
        """
        Called by the SDK once the WebSocket handshake succeeds.

        Two-step subscription protocol (per SDK docs):
          Step 1 — Subscribe to the 'feed' channel (mandatory handshake)
          Step 2 — Request live feed data for specific instruments
        """
        log.info("WebSocket connected. Subscribing to %d instrument(s)…", len(INSTRUMENT_CODES))

        # Reset back-off on successful reconnect
        self._reconnect_delay = RECONNECT_DELAY_MIN

        # ── Step 1: Subscribe to the feed channel ───────────────────────
        subscribe_msg = {
            "action": "subscribe",
            "key":    ["feed"],
            "value":  [""],
        }
        self._sws.subscribe(subscribe_msg)  # type: ignore[union-attr]

        # ── Step 2: Request live LTP feed for all instruments ────────────
        # The instrument list is joined into a single comma-separated string
        # as shown in the official SDK example.
        feed_msg = {
            "action": "feed",
            "key":    ["ltp"],       # use "depth" for full order-book / Level 2
            "value":  [",".join(INSTRUMENT_CODES)],
        }
        self._sws.fetchData(feed_msg)  # type: ignore[union-attr]
        log.info("Subscription sent: %s", ", ".join(INSTRUMENT_CODES))

    def _on_data(self, wsapp: object, message: object) -> None:
        """
        Called for every incoming WebSocket frame.
        `message` is the raw response from Sharekhan (JSON string or 'pong').
        """
        tick = parse_tick(message)
        if tick is not None:
            self._writer.add(tick)

    def _on_error(self, wsapp: object, error: object) -> None:
        """Logs WebSocket-level errors without crashing the process."""
        log.error("WebSocket error: %s", error)

    def _on_close(self, wsapp: object) -> None:
        """
        Called when the server closes the connection.
        The SDK's internal _on_close only passes `wsapp` (not the status
        codes that newer websocket-client versions provide), so no *args needed.
        """
        log.warning("WebSocket closed by server (or network drop).")

    # ── Main reconnect loop ───────────────────────────────────────────────
    def run(self) -> None:
        """
        Blocking entry point.  Keeps reconnecting until request_shutdown()
        is called or the process is killed.
        """
        log.info(
            "Tick harvester starting. Instruments=%d  Batch=%d  DataDir=%s",
            len(self._instrument_codes), BATCH_SIZE, DATA_DIR,
        )

        attempt = 0

        while not self._shutdown.is_set():
            attempt += 1
            log.info(
                "─── Connection attempt #%d (next_backoff=%ds) ───",
                attempt, self._reconnect_delay,
            )

            try:
                # Create a fresh SDK object on every attempt.
                # The Sharekhan SDK does not support re-using a closed instance.
                self._sws = SharekhanWebSocket(self._access_token)

                # Wire up our callbacks
                self._sws.on_open  = self._on_open   # type: ignore[assignment]
                self._sws.on_data  = self._on_data   # type: ignore[assignment]
                self._sws.on_error = self._on_error  # type: ignore[assignment]
                self._sws.on_close = self._on_close  # type: ignore[assignment]

                # BLOCKING — returns only when the WebSocket is closed
                self._sws.connect()

            except KeyboardInterrupt:
                # Ctrl+C on Windows (SIGINT on Linux is handled by signal handler)
                log.info("KeyboardInterrupt caught inside connect() loop.")
                self.request_shutdown()
                break

            except Exception as exc:
                log.error(
                    "Unhandled exception in connect() [attempt #%d]: %s",
                    attempt, exc,
                    exc_info=True,
                )

            # ── Post-disconnect logic ────────────────────────────────────
            if self._shutdown.is_set():
                log.info("Shutdown flag is set — stopping reconnect loop.")
                break

            log.info(
                "Disconnected. Retrying in %ds (back-off max=%ds)…",
                self._reconnect_delay,
                RECONNECT_DELAY_MAX,
            )

            # Sleep in 1-second slices so a shutdown signal can interrupt us
            for _ in range(self._reconnect_delay):
                if self._shutdown.is_set():
                    break
                time.sleep(1)

            # Double the back-off, but cap it
            self._reconnect_delay = min(self._reconnect_delay * 2, RECONNECT_DELAY_MAX)

        log.info("Run loop exited cleanly after %d attempt(s).", attempt)


# ═════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log.info("=" * 60)
    log.info("  Sharekhan Tick Harvester — starting up")
    log.info("=" * 60)

    # ── 1. Load credentials ───────────────────────────────────────────────
    try:
        cfg = load_config()
    except ValueError as exc:
        log.critical("Configuration error: %s", exc)
        raise SystemExit(1) from exc

    log.info("Config loaded — customer_id=%s", cfg.get("customer_id", "N/A"))

    # ── 2. Build the Sharekhan REST client  ───────────────────────────────
    # SharekhanConnect is needed solely for the boot-time master() call.
    # After scrip codes are resolved it is no longer referenced — GC will
    # reclaim the object and free the master-list memory automatically.
    sharekhan_rest = SharekhanConnect(cfg["api_key"], cfg["access_token"])

    # ── 3. Resolve instrument codes dynamically from the master list ──────
    # This is the only network-blocking call at startup.  On a t3.small the
    # NC master list (~10 000 rows) resolves in < 3 s; BC takes ~1 s.
    try:
        instrument_codes = fetch_dynamic_scrip_codes(
            sharekhan=sharekhan_rest,
            symbols_file=FO_SYMBOLS_FILE,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        log.critical("Scrip-code resolution failed: %s", exc)
        raise SystemExit(1) from exc

    # Update the module-level INSTRUMENT_CODES so external tools / health
    # checks that inspect the variable can see the live list.
    INSTRUMENT_CODES[:] = instrument_codes

    log.info(
        "Instrument codes resolved  →  %d code(s).  First 10: %s%s",
        len(instrument_codes),
        ", ".join(instrument_codes[:10]),
        " …" if len(instrument_codes) > 10 else "",
    )

    # ── 4. Initialise the CSV writer ──────────────────────────────────────
    writer = CsvBatchWriter(data_dir=DATA_DIR, batch_size=BATCH_SIZE)

    # ── 5. Initialise the harvester ───────────────────────────────────────
    harvester = TickHarvester(
        access_token=cfg["access_token"],
        writer=writer,
        instrument_codes=instrument_codes,
    )

    # ── 6. Register OS signal handlers ───────────────────────────────────
    #   SIGINT  → Ctrl+C in terminal
    #   SIGTERM → systemd stop / kill command
    #   Both will flush the RAM buffer before the process exits.
    signal.signal(signal.SIGINT,  harvester.request_shutdown)
    signal.signal(signal.SIGTERM, harvester.request_shutdown)

    # ── 7. Start the periodic safety flusher (daemon thread) ──────────────
    flusher = PeriodicFlusher(writer=writer, interval=PERIODIC_FLUSH_INTERVAL)
    flusher.start()

    # ── 8. Run the blocking WebSocket loop ───────────────────────────────
    try:
        harvester.run()
    finally:
        # Ensure flusher thread stops cleanly even if run() exits unexpectedly
        flusher.stop()
        log.info("Periodic flusher stopped. Process exiting.")


if __name__ == "__main__":
    main()
