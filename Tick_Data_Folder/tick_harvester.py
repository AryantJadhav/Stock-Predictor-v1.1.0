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
import smtplib
import threading
import time
from datetime import date, datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
CONFIG_FILE    = _BASE_DIR / "config.json"         # your API credentials
DATA_DIR       = _BASE_DIR / "tick_data"            # root output directory
STOCK_DATA_DIR = DATA_DIR / "stock_price_data"      # NC / BC spot-price CSVs
FO_DATA_DIR    = DATA_DIR / "fo_data"               # NF options / futures CSVs
LOG_FILE        = _BASE_DIR / "harvester.log"    # rotated by the OS or manually
FO_SYMBOLS_FILE           = _BASE_DIR / "fo_symbols.txt"            # spot/equity symbols
FO_OPTIONS_UNDERLYINGS_FILE = _BASE_DIR / "fo_options_underlyings.txt"  # option chain underlyings

# Exchange prefixes used when building WebSocket instrument codes
# NC  – NSE Cash / Index  (spot prices for F&O stocks, NIFTY 50, NIFTY BANK)
# BC  – BSE Cash / Index  (SENSEX)
# NF  – NSE F&O           (options + futures contracts)
EXCHANGE_NSE    = "NC"
EXCHANGE_BSE    = "BC"
EXCHANGE_NSE_FO = "NF"

# Symbols that live on BSE rather than NSE — only SENSEX currently
# (all other F&O underlyings are on NSE).
BSE_ONLY_SYMBOLS: frozenset[str] = frozenset({"SENSEX"})

# How far ahead (in calendar days) to fetch option contracts.
# 60 days covers the current weekly + current monthly + next monthly expiry.
# Increase to 90+ if you also want far-month contracts.
OPTIONS_EXPIRY_LOOKAHEAD_DAYS: int = 60

# CSV flush threshold – at 500 items the buffer is written to disk and cleared
BATCH_SIZE = 500

# Reconnect back-off (seconds)
RECONNECT_DELAY_MIN = 5    # first retry after 5 s
RECONNECT_DELAY_MAX = 60   # cap the back-off at 60 s

# Periodic safety flush – even if the buffer never hits 500 items, flush every
# N seconds so we don't lose data during quiet market periods
PERIODIC_FLUSH_INTERVAL = 60  # seconds

# ─────────────────────────────────────────────────────────────────────────────
#  NSE TRADING HOLIDAYS  ← file-driven, no code change needed
#  Edit  :  Tick_Data_Folder/nse_holidays.txt  (one DD-MM-YYYY date per line)
#  Loaded:  once at startup by _load_nse_holidays()
# ─────────────────────────────────────────────────────────────────────────────
NSE_HOLIDAYS_FILE = _BASE_DIR / "nse_holidays.txt"


def _load_nse_holidays() -> frozenset:
    """
    Parse nse_holidays.txt and return a frozenset of date objects.

    File format (same as the txt file you edit):
      DD-MM-YYYY   Description    <- date + optional description
      # comment line             <- ignored
      <blank line>               <- ignored

    If the file is missing or unreadable a warning is logged and an
    empty frozenset is returned so the harvester still starts.
    """
    holidays: set[date] = set()
    try:
        with open(NSE_HOLIDAYS_FILE, "r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                date_part = line.split()[0]   # first token is DD-MM-YYYY
                try:
                    holidays.add(datetime.strptime(date_part, "%d-%m-%Y").date())
                except ValueError:
                    print(
                        f"WARNING  nse_holidays.txt line {lineno}: "
                        f"cannot parse date {date_part!r} — skipped."
                    )
    except FileNotFoundError:
        print(f"WARNING  nse_holidays.txt not found at {NSE_HOLIDAYS_FILE} — no holidays loaded.")
    except OSError as exc:
        print(f"WARNING  Could not read nse_holidays.txt: {exc}")

    print(f"INFO     Loaded {len(holidays)} NSE holiday(s) from nse_holidays.txt.")
    return frozenset(holidays)


# Loaded once at import/startup; used by _is_trading_day()
NSE_HOLIDAYS: frozenset = _load_nse_holidays()


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
# Column order is fixed — CsvBatchWriter uses DictWriter(extrasaction="ignore")
# so unknown keys from the API payload are silently dropped.
CSV_HEADERS: list[str] = [
    "Timestamp",   # ISO-8601 local timestamp with milliseconds
    "Exchange",    # e.g. NC (NSE Cash) or BC (BSE Cash)
    "Symbol",      # TradingSymbol e.g. RELIANCE, NIFTY
    "LTP",         # Last Traded Price
    "Open",        # Day open price
    "High",        # Day high price
    "Low",         # Day low price
    "Close",       # Previous close / settlement price
    "Volume",      # Total traded quantity for the day
    "VWAP",        # Volume-Weighted Average Price (avgTradedPrice / ATP)
    "Best_Bid",    # Best bid price (top of order book)
    "Best_Ask",    # Best ask / offer price (top of order book)
    "OI",          # Open Interest (relevant for F&O underlyings)
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
#  BOOT-SEQUENCE ALERT MAILER
# ═════════════════════════════════════════════════════════════════════════════
def send_boot_warning(unmatched: set[str], cfg: dict) -> None:
    """
    Send an email alert listing symbols that could not be resolved against
    the Sharekhan master list during the boot sequence.

    Credentials are read from config.json keys:
        smtp_sender    – Gmail / Outlook address used to send (e.g. bot@gmail.com)
        smtp_receiver  – Destination address (e.g. you@gmail.com)
        smtp_password  – App Password generated in your Google Account settings
                         (NOT your normal login password)
        smtp_host      – Optional. Default: smtp.gmail.com
        smtp_port      – Optional. Default: 587  (STARTTLS)

    Fault-tolerant: any failure (network, bad credentials, missing keys) is
    logged locally and the function returns quietly — the harvester continues.
    """
    sender   = cfg.get("smtp_sender",   "").strip()
    receiver = cfg.get("smtp_receiver", "").strip()
    password = cfg.get("smtp_password", "").strip()

    if not sender or not receiver or not password:
        log.warning(
            "Boot-warning email NOT sent: smtp_sender / smtp_receiver / smtp_password "
            "are missing in config.json. Add them to enable email alerts."
        )
        return

    smtp_host = cfg.get("smtp_host", "smtp.gmail.com").strip()
    smtp_port = int(cfg.get("smtp_port", 587))

    sorted_syms = sorted(unmatched)
    bullet_list = "\n".join(f"  • {s}" for s in sorted_syms)

    subject = "[ALERT] Tick Harvester: Unmatched Symbols"
    body = (
        f"Boot-time scrip-code resolution completed with {len(sorted_syms)} "
        f"unmatched symbol(s).\n\n"
        f"The following symbol(s) from fo_symbols.txt could NOT be found "
        f"in the Sharekhan NC/BC master list:\n\n"
        f"{bullet_list}\n\n"
        f"--- Action Required ---\n"
        f"1. Open Tick_Data_Folder/fo_symbols.txt on your EC2 instance.\n"
        f"2. Check that each failing symbol matches the exact 'TradingSymbol' "
        f"string in sharekhan.master('NC') (case-sensitive after uppercasing).\n"
        f"3. Run the verification snippet at the top of fo_symbols.txt to dump "
        f"the live master and compare.\n\n"
        f"The harvester has continued with the {len(sorted_syms)} unmatched "
        f"symbol(s) excluded from the WebSocket subscription.\n"
    )

    msg = MIMEMultipart()
    msg["From"]    = sender
    msg["To"]      = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        log.info("Boot-warning email sent to %s (%d unmatched symbol(s)).", receiver, len(sorted_syms))
    except smtplib.SMTPAuthenticationError:
        log.error(
            "Boot-warning email FAILED: authentication error. "
            "Check smtp_sender / smtp_password in config.json. "
            "For Gmail use an App Password, not your account password."
        )
    except (smtplib.SMTPException, OSError, TimeoutError) as exc:
        log.error("Boot-warning email FAILED: %s. Harvester will continue.", exc)

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
    2.  Call sharekhan.master("NC") ← NSE CASH MARKET (spot / equity prices).
        This is intentionally the Cash segment, NOT NF (NSE F&O derivatives).
        Subscribing to NC codes streams the live SPOT price of each underlying
        (e.g. RELIANCE equity price on NSE), which is what we want for Greeks
        calculation and IV computation in the post-processing step.
        Matches every row whose tradingSymbol is in target_symbols, covering
        all 226 F&O-eligible equities + NIFTY 50 + NIFTY BANK indices.
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

    # ── Step 3: Warn about unresolved symbols + send email alert ─────────
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
        # Load config to get SMTP credentials (idempotent — re-reads the file).
        try:
            _alert_cfg = load_config()
        except Exception:
            _alert_cfg = {}
        send_boot_warning(unresolved, _alert_cfg)

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
#  F&O OPTIONS CHAIN SCRIP-CODE FETCHER  (NF segment)
# ═════════════════════════════════════════════════════════════════════════════

def _parse_expiry_date(raw: str) -> date | None:
    """
    Parse an expiry date string from the NF master into a date object.
    Handles multiple formats used by Indian broker APIs.
    Returns None if parsing fails.
    """
    raw = raw.strip()
    # Strip time component: "2026-03-27T00:00:00" → "2026-03-27"
    for sep in ("T", " "):
        if sep in raw:
            raw = raw.split(sep)[0]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%b%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def fetch_fo_option_codes(
    sharekhan: SharekhanConnect,
    options_file: Path = FO_OPTIONS_UNDERLYINGS_FILE,
) -> list[str]:
    """
    Boot-time resolver for NSE F&O options chain scrip codes.

    Algorithm
    ---------
    1.  Read fo_options_underlyings.txt to get the list of underlyings for
        which you want options data (e.g. NIFTY, BANKNIFTY, RELIANCE).
    2.  Call sharekhan.master("NF") — the full NSE F&O segment master.
        This list contains every option + futures contract currently listed
        (100 000+ rows; iterated in pure Python, O(n) time, freed after use).
    3.  Filter to rows where:
          a.  TradingSymbol ends with CE or PE  → options only, not futures
          b.  TradingSymbol starts with a target underlying
          c.  ExpiryDate falls within today … today + OPTIONS_EXPIRY_LOOKAHEAD_DAYS
    4.  Build "NF{scripCode}" strings and return the list.

    RAM note
    --------
    The NF master is a large one-time payload.  After this function returns
    the raw list is eligible for garbage collection.  Only the tiny list of
    matched scrip-code strings is retained in memory.

    The parse_tick() and CsvBatchWriter already handle NF ticks generically
    — no other changes are needed for options data to flow into the CSV.
    """
    if not options_file.exists():
        log.info(
            "fo_options_underlyings.txt not found — NF options subscription skipped.\n"
            "  Create %s with one underlying per line (e.g. NIFTY) to enable.",
            options_file,
        )
        return []

    target_underlyings: set[str] = set()
    with open(options_file, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            target_underlyings.add(line.upper())

    if not target_underlyings:
        log.info("fo_options_underlyings.txt is empty — NF options subscription skipped.")
        return []

    log.info(
        "Fetching NSE F&O master (exchange=NF) for %d underlying(s): %s …",
        len(target_underlyings),
        ", ".join(sorted(target_underlyings)),
    )

    try:
        raw_nf = sharekhan.master(EXCHANGE_NSE_FO)
    except Exception as exc:
        log.error("sharekhan.master('NF') failed: %s — skipping NF options.", exc)
        return []

    nf_records: list[dict] = _normalise_master_response(raw_nf, EXCHANGE_NSE_FO)
    log.info("  NF master rows received: %d", len(nf_records))

    today  = date.today()
    cutoff = today + timedelta(days=OPTIONS_EXPIRY_LOOKAHEAD_DAYS)

    instrument_codes: list[str] = []

    for record in nf_records:
        sym_raw = _get_field(
            record,
            "TradingSymbol", "tradingSymbol", "tradingsymbol",
            "Symbol", "symbol",
        )
        if not sym_raw:
            continue

        sym = sym_raw.strip().upper()

        # ── Filter 1: options only (CE / PE suffix) — skip futures ────────
        if not (sym.endswith("CE") or sym.endswith("PE")):
            continue

        # ── Filter 2: underlying must be in our target list ───────────────
        # TradingSymbol for an option starts with the underlying name, e.g.:
        #   NIFTY27MAR2524000CE  → underlying = NIFTY
        #   RELIANCE27MAR252500CE → underlying = RELIANCE
        matched = any(sym.startswith(und) for und in target_underlyings)
        if not matched:
            continue

        # ── Filter 3: expiry within the lookahead window ──────────────────
        expiry_raw = _get_field(
            record,
            "ExpiryDate", "expiryDate", "Expiry", "expiry",
            "ExpiryDateTime", "MaturityDate", "expiry_date",
        )
        if expiry_raw:
            expiry_dt = _parse_expiry_date(expiry_raw)
            if expiry_dt is None or expiry_dt < today or expiry_dt > cutoff:
                continue
        # If the master record has no expiry field, include it anyway

        # ── Scrip code ───────────────────────────────────────────────────
        code = _get_field(
            record,
            "ScripCode", "scripCode", "scripcode", "SCRIPCODE",
            "Code", "code", "scripId", "ScripId",
        )
        if not code:
            continue

        instrument_codes.append(f"{EXCHANGE_NSE_FO}{code}")

    log.info(
        "  NF options matched: %d contract(s) expiring within %d days.",
        len(instrument_codes),
        OPTIONS_EXPIRY_LOOKAHEAD_DAYS,
    )
    return instrument_codes


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
        self._buffer: dict[str, list[dict]] = {}  # symbol → [ticks]
        self._lock         = threading.Lock()
        self._current_date = date.today()
        self._dir.mkdir(parents=True, exist_ok=True)
        log.info("CsvBatchWriter ready — writing to %s (batch=%d)", self._dir, batch_size)

    # ──────────────────────────────────────────────────────────────────────
    def _csv_path(self, symbol: str) -> Path:
        """
        Return the path for a symbol's CSV file, inside a DD-MM-YYYY subfolder.
        Detects a date change (midnight rollover), creates the new folder, and logs it.
        Structure: <data_dir>/<DD-MM-YYYY>/<SYMBOL>.csv
        """
        today = date.today()
        if today != self._current_date:
            log.info(
                "Midnight rollover: switching from %s → %s",
                self._current_date.strftime("%d-%m-%Y"),
                today.strftime("%d-%m-%Y"),
            )
            self._current_date = today
        day_dir = self._dir / self._current_date.strftime("%d-%m-%Y")
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir / f"{symbol}.csv"

    # ──────────────────────────────────────────────────────────────────────
    def _flush_symbol_locked(self, symbol: str) -> None:
        """
        Write one symbol's buffer to its CSV file and clear it.
        MUST be called while self._lock is already held.
        """
        ticks = self._buffer.get(symbol)
        if not ticks:
            return
        target = self._csv_path(symbol)
        file_existed = target.exists()
        n = len(ticks)
        try:
            with open(target, "a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=CSV_HEADERS,
                    extrasaction="ignore",
                )
                if not file_existed:
                    writer.writeheader()
                writer.writerows(ticks)
            ticks.clear()
            log.debug("Flushed %d ticks → %s", n, target.name)
        except OSError as exc:
            log.error("Disk write failed (%s). Buffer retained (%d items).", exc, n)

    # ──────────────────────────────────────────────────────────────────────
    def _flush_locked(self) -> None:
        """
        Flush all symbols' buffers to disk.
        MUST be called while self._lock is already held.
        """
        for symbol in list(self._buffer):
            self._flush_symbol_locked(symbol)

    # ──────────────────────────────────────────────────────────────────────
    def add(self, tick: dict) -> None:
        """
        Add one tick to the in-memory buffer, keyed by symbol.
        Triggers an automatic flush for that symbol when its buffer reaches batch_size.
        """
        symbol = tick.get("Symbol") or "UNKNOWN"
        with self._lock:
            buf = self._buffer.setdefault(symbol, [])
            buf.append(tick)
            if len(buf) >= self._batch_size:
                self._flush_symbol_locked(symbol)

    # ──────────────────────────────────────────────────────────────────────
    def flush(self) -> None:
        """
        Force-flush all symbol buffers to disk regardless of their current size.
        Called by the periodic safety timer and on shutdown.
        """
        with self._lock:
            count = sum(len(v) for v in self._buffer.values())
            self._flush_locked()
            if count:
                log.info("Safety flush: wrote %d buffered ticks to disk.", count)

    # ──────────────────────────────────────────────────────────────────────
    def __len__(self) -> int:
        """Return total number of ticks currently held in RAM across all symbols."""
        with self._lock:
            return sum(len(v) for v in self._buffer.values())


# ═════════════════════════════════════════════════════════════════════════════
#  TICK PARSER
# ═════════════════════════════════════════════════════════════════════════════
def parse_tick(raw_message: object) -> dict | None:
    """
    Convert a raw WebSocket message from Sharekhan into a flat dict
    that maps directly to CSV_HEADERS.

    Extracted fields
    ----------------
    Timestamp   – wall-clock at receipt (IST, ISO-8601 with ms)
    Exchange    – NC (NSE Cash) or BC (BSE Cash)
    Symbol      – TradingSymbol string, e.g. RELIANCE, NIFTY
    LTP         – Last Traded Price
    Open        – Day open price
    High        – Day high price
    Low         – Day low price
    Close       – Previous close / settlement price
    Volume      – Total traded quantity (TTQ) for the session
    VWAP        – Volume-Weighted Avg Price (avgTradedPrice / ATP)
    Best_Bid    – Best bid price from the top-of-book (depth feed)
    Best_Ask    – Best ask/offer price from the top-of-book (depth feed)
    OI          – Open Interest

    All fields use safe .get() calls with None as the default so a missing
    key never raises an exception.  Heartbeats ("pong") are silently dropped.
    All parsing failures are logged as warnings and return None so the
    harvester keeps running without crashing.
    """
    try:
        # Heartbeat / keepalive frames — not data ticks, drop silently
        if raw_message is None or raw_message in ("pong", "heartbeat"):
            return None

        # The SDK may deliver the payload as a JSON string or a pre-parsed dict
        data: dict = (
            json.loads(raw_message)
            if isinstance(raw_message, str)
            else raw_message
        )

        if not isinstance(data, dict):
            # Could be a list wrapper or an error envelope — skip silently
            log.debug("Non-dict tick skipped: %s", str(raw_message)[:120])
            return None

        # ── Best Bid / Best Ask ─────────────────────────────────────────────
        # The depth feed nests order-book data under several possible keys.
        # We try the most common shapes defensively.
        #
        # Shape A  (flat):    data["bestBidPrice"]  / data["bestAskPrice"]
        # Shape B  (nested):  data["depth"]["buy"][0]["price"]
        #                     data["depth"]["sell"][0]["price"]
        # Shape C  (list):    data["buyDepth"][0]["price"]
        #                     data["sellDepth"][0]["price"]

        best_bid: object = (
            data.get("bestBidPrice")
            or data.get("bidPrice")
            or data.get("best_bid_price")
            or _nested_get(data, "depth", "buy",  0, "price")
            or _nested_get(data, "buyDepth",       0, "price")
            or None
        )

        best_ask: object = (
            data.get("bestAskPrice")
            or data.get("bestOfferPrice")
            or data.get("askPrice")
            or data.get("best_ask_price")
            or _nested_get(data, "depth", "sell", 0, "price")
            or _nested_get(data, "sellDepth",      0, "price")
            or None
        )

        tick: dict = {
            "Timestamp": datetime.now().isoformat(timespec="milliseconds"),

            # Identification
            "Exchange": data.get("exchange") or None,
            "Symbol":   data.get("tradingSymbol") or data.get("symbol") or None,

            # Price — try multiple known Sharekhan field names
            "LTP":   data.get("ltp")   or data.get("lastTradedPrice")  or data.get("LastTradedPrice") or None,
            "Open":  data.get("open")  or data.get("openPrice")        or data.get("openRate")        or None,
            "High":  data.get("high")  or data.get("highPrice")        or data.get("dayHigh")         or None,
            "Low":   data.get("low")   or data.get("lowPrice")         or data.get("dayLow")          or None,
            "Close": data.get("close") or data.get("closePrice")       or data.get("prevClose")       or None,

            # Volume
            "Volume": (
                data.get("volume")
                or data.get("totalTradeQty")
                or data.get("tradedVolume")
                or data.get("ttq")
                or None
            ),

            # VWAP — Sharekhan calls this avgTradedPrice or ATP
            "VWAP": (
                data.get("avgTradedPrice")
                or data.get("atp")
                or data.get("vwap")
                or data.get("averagePrice")
                or data.get("averageTradedPrice")
                or None
            ),

            # Order-book top (requires depth feed subscription)
            "Best_Bid": best_bid,
            "Best_Ask": best_ask,

            # Open Interest
            "OI": data.get("oi") or data.get("openInterest") or data.get("OpenInterest") or None,
        }

        # Drop empty/useless ticks (e.g. SDK connection-ack messages with no data)
        if tick["Symbol"] is None and tick["LTP"] is None:
            return None

        return tick

    except (json.JSONDecodeError, AttributeError, TypeError) as exc:
        log.warning("Tick parse error: %s | raw=%s", exc, str(raw_message)[:200])
        return None


def _nested_get(obj: object, *keys: object) -> object:
    """
    Safely traverse nested dicts/lists using a sequence of keys/indices.
    Returns None at the first missing key, bad index, or wrong type.

    Example:
        _nested_get(data, "depth", "buy", 0, "price")
        # → data["depth"]["buy"][0]["price"]  or None
    """
    cur = obj
    for key in keys:
        try:
            cur = cur[key]  # type: ignore[index]
        except (KeyError, IndexError, TypeError):
            return None
    return cur


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

    def __init__(self, writers: list[CsvBatchWriter], interval: int = PERIODIC_FLUSH_INTERVAL) -> None:
        super().__init__(name="periodic-flusher", daemon=True)
        self._writers  = writers
        self._interval = interval
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        log.info("PeriodicFlusher started (interval=%ds).", self._interval)
        while not self._stop_evt.wait(timeout=self._interval):
            for w in self._writers:
                w.flush()
        log.info("PeriodicFlusher stopped.")


# ═════════════════════════════════════════════════════════════════════════════
#  TOKEN REMINDER THREAD
# ═════════════════════════════════════════════════════════════════════════════
def _is_trading_day(d: date) -> bool:
    """Return True if `d` is an NSE trading day (not a weekend or listed holiday)."""
    if d.weekday() in (5, 6):          # Saturday=5, Sunday=6
        return False
    return d not in NSE_HOLIDAYS


def _next_trading_day(from_date: date) -> date:
    """Return the first trading day strictly after `from_date`."""
    candidate = from_date + timedelta(days=1)
    while not _is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _sleep_until_next_trading_day() -> None:
    """
    Block (interruptibly) until 00:00:30 on the next trading day.

    Called when the harvester boots on a holiday or weekend so the process
    stays alive (systemd does not restart it) and wakes up automatically
    on the next market day.
    Sleeps in 60-second slices so a SIGINT/SIGTERM still exits promptly.
    """
    today      = date.today()
    next_day   = _next_trading_day(today)
    # Wake up 30 seconds after midnight of the next trading day
    wake_dt    = datetime.combine(next_day, datetime.min.time()) + timedelta(seconds=30)
    wait_secs  = max(0.0, (wake_dt - datetime.now()).total_seconds())

    log.info(
        "Today (%s) is not a trading day.  Sleeping until %s (~%.1f hours).",
        today.isoformat(), wake_dt.strftime("%Y-%m-%d %H:%M:%S"), wait_secs / 3600,
    )
    deadline = time.monotonic() + wait_secs
    while time.monotonic() < deadline:
        # Sleep in 60-second slices so Ctrl+C / SIGTERM is responsive
        remaining = deadline - time.monotonic()
        time.sleep(min(60.0, remaining))


def _is_token_fresh() -> bool:
    """
    Return True if the access_token in config.json was updated today.

    Detection strategy (first that succeeds wins):
      1. If access_token is a dict with a 'timestamp' field
         (format returned by auth_helper.py), parse that date and compare
         to today's local date.
      2. If access_token is a plain string, fall back to the file's
         last-modified date.
      3. On any error (missing file, bad JSON) return False so the
         reminder is sent rather than silently skipped.
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        token = cfg.get("access_token", "")

        # Strategy 1: dict with timestamp (auth_helper.py writes this shape)
        if isinstance(token, dict):
            ts_raw = token.get("timestamp", "")
            if ts_raw:
                # Strip timezone offset so strptime can parse it simply
                ts_clean = ts_raw[:10]   # "2026-03-05T..." → "2026-03-05"
                try:
                    token_date = datetime.strptime(ts_clean, "%Y-%m-%d").date()
                    return token_date == date.today()
                except ValueError:
                    pass

        # Strategy 2: plain string token — use file modification time
        mtime = CONFIG_FILE.stat().st_mtime
        modified_date = datetime.fromtimestamp(mtime).date()
        return modified_date == date.today()

    except Exception:
        return False   # assume stale; send the reminder


def _send_token_reminder(cfg: dict) -> None:
    """
    Send an email reminding the user to refresh the Sharekhan access_token.
    Uses the same SMTP credentials as send_boot_warning().
    Fault-tolerant: any failure is logged and the thread continues.
    """
    sender   = cfg.get("smtp_sender",   "").strip()
    receiver = cfg.get("smtp_receiver", "").strip()
    password = cfg.get("smtp_password", "").strip()

    if not sender or not receiver or not password:
        log.warning("Token reminder email NOT sent: SMTP credentials missing in config.json.")
        return

    smtp_host = cfg.get("smtp_host", "smtp.gmail.com").strip()
    smtp_port = int(cfg.get("smtp_port", 587))

    now_str = datetime.now().strftime("%d-%b-%Y %I:%M %p")
    subject = "[REMINDER] Sharekhan Token Expires at Midnight — Refresh Now"
    body = (
        f"Reminder sent at {now_str}\n\n"
        f"Your Sharekhan access_token expires at midnight IST tonight.\n\n"
        f"--- Action Required Before Market Open (09:15 IST) ---\n"
        f"1. SSH into your EC2 instance (or open a terminal locally).\n"
        f"2. Run:  python auth_helper.py\n"
        f"3. Follow the on-screen steps to get a fresh RequestToken and\n"
        f"   save the new access_token to config.json.\n\n"
        f"This reminder will repeat every 30 minutes until the token is\n"
        f"updated or midnight passes.\n"
    )

    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart()
    msg["From"]    = sender
    msg["To"]      = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        log.info("Token reminder email sent to %s.", receiver)
    except smtplib.SMTPAuthenticationError:
        log.error("Token reminder email FAILED: SMTP authentication error.")
    except (smtplib.SMTPException, OSError, TimeoutError) as exc:
        log.error("Token reminder email FAILED: %s", exc)


class TokenReminderThread(threading.Thread):
    """
    Daemon background thread that emails a token-refresh reminder at 23:00
    (11 PM local time) on Sunday through Thursday, *skipping* evenings that
    precede an NSE market holiday.

    Schedule
    --------
    Indian markets are open Monday–Friday (excluding holidays).  Reminders
    fire the evening before each trading day so the token is refreshed
    overnight:
      Sunday    23:00  →  refresh for Monday
      Monday    23:00  →  refresh for Tuesday
      Tuesday   23:00  →  refresh for Wednesday
      Wednesday 23:00  →  refresh for Thursday
      Thursday  23:00  →  refresh for Friday   ← covers Friday trading
    Saturday: no reminder (no trading day follows on Sunday).

    Holiday awareness
    -----------------
    If the *next* calendar day is an NSE holiday (e.g. Monday is Holi),
    the Sunday-night reminder is automatically skipped — no token refresh
    is needed until the day before the next actual trading day.
    The holiday list is read from nse_holidays.txt (NSE_HOLIDAYS).

    Repetition
    ----------
    After the first reminder at 23:00, the thread re-checks every
    RECHECK_MINUTES.  If the token is still stale it sends another
    reminder.  It stops sending once either:
      a)  _is_token_fresh() returns True (token was updated), or
      b)  Local time crosses midnight (new calendar day begins).

    Python weekday(): Mon=0 Tue=1 Wed=2 Thu=3 Fri=4 Sat=5 Sun=6
    Reminder days  : Sun=6, Mon=0, Tue=1, Wed=2, Thu=3
    """

    REMINDER_HOUR      = 23             # 11 PM
    REMINDER_WEEKDAYS  = {6, 0, 1, 2, 3}  # Sun to Thu
    RECHECK_MINUTES    = 30

    def __init__(self, cfg: dict) -> None:
        super().__init__(name="token-reminder", daemon=True)
        self._cfg      = cfg
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    # ── internal helpers ────────────────────────────────────────────────────
    def _seconds_until_next_trigger(self) -> float:
        """
        Return the number of seconds to sleep before the next 23:00 trigger
        on a reminder day.  Sleeps in 30-second slices so stop() is responsive.
        """
        now = datetime.now()
        today_trigger = now.replace(hour=self.REMINDER_HOUR, minute=0,
                                    second=0, microsecond=0)

        # Possible next trigger times: today 23:00 (if in future) + up to 14 days ahead
        for delta_days in range(15):
            candidate = today_trigger + timedelta(days=delta_days)
            if candidate <= now:
                continue
            if candidate.weekday() not in self.REMINDER_WEEKDAYS:
                continue
            # The reminder covers the *next* calendar day — skip if it's not a trading day
            next_day = candidate.date() + timedelta(days=1)
            if not _is_trading_day(next_day):
                log.debug(
                    "TokenReminderThread: skipping %s 23:00 reminder — %s is a holiday/weekend.",
                    candidate.strftime("%A"), next_day.isoformat(),
                )
                continue
            return (candidate - now).total_seconds()

        # Fallback: check again in 24 hours (should never reach here)
        return 86400.0

    def _sleep_interruptible(self, seconds: float) -> bool:
        """
        Sleep for `seconds` in 30-second slices.
        Returns True if the stop event was set during sleep.
        """
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self._stop_evt.wait(timeout=30):
                return True
        return False

    # ── main loop ───────────────────────────────────────────────────────────
    def run(self) -> None:
        log.info(
            "TokenReminderThread started. Reminders at %02d:00 on Sun–Thu.",
            self.REMINDER_HOUR,
        )
        while not self._stop_evt.is_set():
            # ── 1. Sleep until the next 23:00 on a reminder day ──────────
            wait_secs = self._seconds_until_next_trigger()
            log.info(
                "TokenReminderThread: next reminder in %.1f hours.",
                wait_secs / 3600,
            )
            if self._sleep_interruptible(wait_secs):
                break   # stop() was called

            # ── 2. Trigger reached: send + recheck loop until token fresh or midnight
            reminder_date = date.today()
            while not self._stop_evt.is_set():
                if date.today() != reminder_date:
                    # Midnight crossed — stop nagging for tonight
                    log.info("TokenReminderThread: midnight passed, stopping nag loop.")
                    break

                if _is_token_fresh():
                    log.info(
                        "TokenReminderThread: token already updated today — reminder skipped."
                    )
                    break

                # Token is stale — send reminder
                try:
                    _alert_cfg = load_config()
                except Exception:
                    _alert_cfg = self._cfg
                _send_token_reminder(_alert_cfg)

                # Wait RECHECK_MINUTES before checking again
                if self._sleep_interruptible(self.RECHECK_MINUTES * 60):
                    break

        log.info("TokenReminderThread stopped.")
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
        stock_writer:     CsvBatchWriter,
        fo_writer:        CsvBatchWriter,
        instrument_codes: list[str],
    ) -> None:
        self._access_token     = access_token
        self._stock_writer     = stock_writer   # NC / BC spot-price ticks
        self._fo_writer        = fo_writer      # NF options / futures ticks
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
        buffered = len(self._stock_writer) + len(self._fo_writer)
        log.info("Shutdown requested (%s). Flushing %d buffered ticks…", sig_name, buffered)
        self._shutdown.set()

        # Tell the running WebSocket to close cleanly
        if self._sws is not None:
            try:
                self._sws.close_connection()
            except Exception as exc:
                log.debug("close_connection() raised (harmless): %s", exc)

        # Flush whatever is left in RAM to disk
        self._stock_writer.flush()
        self._fo_writer.flush()
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

        # ── Step 2: Request full depth feed for all instruments ──────────
        # Feed key options:
        #   "ltp"   — Last Traded Price only (minimal payload)
        #   "quote" — LTP + OHLC + Volume (no order-book)
        #   "depth" — Full market depth: LTP + OHLC + Volume + VWAP +
        #             best-bid/ask from the order book  ← we use this
        #
        # "depth" is required to capture VWAP (avgTradedPrice) and
        # Best_Bid / Best_Ask fields in the WebSocket payload.
        # The instrument list is a single comma-separated string per SDK docs.
        feed_msg = {
            "action": "feed",
            "key":    ["depth"],
            "value":  [",".join(INSTRUMENT_CODES)],
        }
        self._sws.fetchData(feed_msg)  # type: ignore[union-attr]
        log.info("Depth-feed subscription sent for %d instrument(s).", len(INSTRUMENT_CODES))

    def _on_data(self, wsapp: object, message: object) -> None:
        """
        Called for every incoming WebSocket frame.
        `message` is the raw response from Sharekhan (JSON string or 'pong').
        """
        tick = parse_tick(message)
        if tick is not None:
            if tick.get("Exchange") == EXCHANGE_NSE_FO:
                self._fo_writer.add(tick)
            else:
                self._stock_writer.add(tick)

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
            "Tick harvester starting. Instruments=%d  Batch=%d  StockDir=%s  FoDir=%s",
            len(self._instrument_codes), BATCH_SIZE, STOCK_DATA_DIR, FO_DATA_DIR,
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
    # ── 0. Holiday / weekend guard ────────────────────────────────────────
    # If the process starts (or loops back here after a holiday sleep) on a
    # non-trading day, sleep until 00:00:30 of the next trading day and then
    # re-enter the boot sequence.  The outer while-loop means the process
    # never exits on its own — systemd does not need to restart it.
    while not _is_trading_day(date.today()):
        _sleep_until_next_trading_day()
        # Re-check after waking (handles consecutive holidays, e.g. Thu holiday + Fri holiday)

    log.info("=" * 60)
    log.info("  Sharekhan Tick Harvester — starting up")
    log.info("=" * 60)

    # Log the effective timezone so EC2 misconfigurations are caught immediately.
    # If you see UTC here instead of IST, set TZ=Asia/Kolkata in the systemd
    # service file OR run: sudo timedatectl set-timezone Asia/Kolkata
    import time as _time
    log.info(
        "Timezone: %s  |  Local time: %s",
        _time.tzname[_time.daylight],
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

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

    # ── 3b. Resolve NF options chain scrip codes ──────────────────────────
    # Reads fo_options_underlyings.txt; silently skips if the file is absent.
    fo_option_codes = fetch_fo_option_codes(
        sharekhan=sharekhan_rest,
        options_file=FO_OPTIONS_UNDERLYINGS_FILE,
    )
    if fo_option_codes:
        log.info(
            "Adding %d NF option contract(s) to subscription.",
            len(fo_option_codes),
        )
        instrument_codes.extend(fo_option_codes)

    # Update the module-level INSTRUMENT_CODES so external tools / health
    # checks that inspect the variable can see the live list.
    INSTRUMENT_CODES[:] = instrument_codes

    log.info(
        "Total instruments  →  %d code(s)  (spot: %d,  options: %d).  First 10: %s%s",
        len(instrument_codes),
        len(instrument_codes) - len(fo_option_codes),
        len(fo_option_codes),
        ", ".join(instrument_codes[:10]),
        " …" if len(instrument_codes) > 10 else "",
    )

    # ── 4. Initialise the CSV writers (one per data category) ───────────
    stock_writer = CsvBatchWriter(data_dir=STOCK_DATA_DIR, batch_size=BATCH_SIZE)
    fo_writer    = CsvBatchWriter(data_dir=FO_DATA_DIR,    batch_size=BATCH_SIZE)

    # ── 5. Initialise the harvester ───────────────────────────────────────
    harvester = TickHarvester(
        access_token=cfg["access_token"],
        stock_writer=stock_writer,
        fo_writer=fo_writer,
        instrument_codes=instrument_codes,
    )

    # ── 6. Register OS signal handlers ───────────────────────────────────
    #   SIGINT  → Ctrl+C in terminal
    #   SIGTERM → systemd stop / kill command
    #   Both will flush the RAM buffer before the process exits.
    signal.signal(signal.SIGINT,  harvester.request_shutdown)
    signal.signal(signal.SIGTERM, harvester.request_shutdown)

    # ── 7. Start the periodic safety flusher (daemon thread) ──────────────
    flusher = PeriodicFlusher(writers=[stock_writer, fo_writer], interval=PERIODIC_FLUSH_INTERVAL)
    flusher.start()

    # ── 8. Start the token reminder thread (daemon thread) ────────────────
    # Emails a reminder at 23:00 Sun–Thu if the access_token is still stale.
    # Repeats every 30 minutes until the token is refreshed or midnight passes.
    reminder = TokenReminderThread(cfg=cfg)
    reminder.start()

    # ── 9. Run the blocking WebSocket loop ───────────────────────────────
    try:
        harvester.run()
    finally:
        flusher.stop()
        reminder.stop()
        log.info("Background threads stopped. Process exiting.")


if __name__ == "__main__":
    main()
