#!/usr/bin/env python3
"""
iv_greeks_enricher.py
=====================
Standalone post-market enrichment script for Indian F&O options data.

Reads one or more CSV files containing raw option tick/minute data, then
calculates and appends:
  • Implied Volatility  (IV) via Black-Scholes inversion
  • Delta, Gamma, Theta (per calendar day), Vega (per 1 % IV), Rho (per 1 % r)

Designed for a standard Windows desktop PC — no RAM constraints, full pandas
vectorisation used where the math allows it.

────────────────────────────────────────────────────────────────────────────────
EXPECTED INPUT CSV SCHEMA
────────────────────────────────────────────────────────────────────────────────
Column name           | Example value           | Notes
──────────────────────|─────────────────────────|──────────────────────────────
Timestamp             | 2026-03-03 09:16:01.123 | See TIMESTAMP_FORMATS below
Symbol                | NIFTY24MAR22100CE        | NSE / Sharekhan option code
LTP                   | 148.50                   | Option last traded price
Best_Bid_Or_Ask       | 148.00                   | Used for IV over LTP if present
OI                    | 1200000                  | Open interest (passed through)
Underlying_LTP        | 22150.75                 | OPTIONAL – spot price in-file

Column names are case-insensitive and leading/trailing spaces are stripped.

────────────────────────────────────────────────────────────────────────────────
UNDERLYING PRICE RESOLUTION  (two methods — first valid wins)
────────────────────────────────────────────────────────────────────────────────
  Method 1 (in-file)   : a column named "Underlying_LTP" (or alias) in the CSV.
  Method 2 (dict)      : UNDERLYING_PRICES dict at the top of this script — set
                         your day's closing spot/mid prices here before running.
  If neither source has a value, the row is flagged UNDERLYING_MISSING.

────────────────────────────────────────────────────────────────────────────────
USAGE
────────────────────────────────────────────────────────────────────────────────
  # Process all CSVs in the default input folder
  python iv_greeks_enricher.py

  # Single file
  python iv_greeks_enricher.py --input data\\options_03-03-2026.csv

  # Folder + custom output dir + custom rate
  python iv_greeks_enricher.py --input data\\ --output results\\ --rate 0.068

  # Verbose mode (shows every skipped row)
  python iv_greeks_enricher.py --verbose

────────────────────────────────────────────────────────────────────────────────
DEPENDENCIES
────────────────────────────────────────────────────────────────────────────────
  pip install py_vollib pandas numpy
"""

# ─────────────────────────────────────────────────────────────────────────────
#  Standard library
# ─────────────────────────────────────────────────────────────────────────────
import argparse
import logging
import math
import re
import sys
import time
import warnings
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
#  Third-party
# ─────────────────────────────────────────────────────────────────────────────
try:
    import pandas as pd
    import numpy as np
except ImportError:
    sys.exit("ERROR: Run  pip install pandas numpy")

try:
    # Black-Scholes IV solver
    from py_vollib.black_scholes.implied_volatility import (
        implied_volatility as _bsiv,
    )
    # Analytical Greeks (closed-form, vectorisable manually)
    from py_vollib.black_scholes.greeks.analytical import (
        delta as _delta,
        gamma as _gamma,
        theta as _theta,   # returned as $/calendar day (already ÷365)
        vega  as _vega,    # returned as $/1 % IV move  (already ÷100)
        rho   as _rho,     # returned as $/1 % rate move (already ÷100)
    )
except ImportError:
    sys.exit("ERROR: Run  pip install py_vollib")

# Suppress the noisy RuntimeWarning py_vollib emits when its
# Brent-solver bracket fails during the IV search
warnings.filterwarnings("ignore", category=RuntimeWarning)


# ══════════════════════════════════════════════════════════════════════════════
#  ███  USER CONFIGURATION  ███  — edit this section before each run
# ══════════════════════════════════════════════════════════════════════════════

# ── Default I/O paths ─────────────────────────────────────────────────────────
_HERE            = Path(__file__).parent
DEFAULT_INPUT    = _HERE / "input_data"       # folder with raw CSVs
DEFAULT_OUTPUT   = _HERE / "enriched_output"  # enriched CSVs written here
OUTPUT_SUFFIX    = "_enriched"                # appended before .csv

# ── Financial parameters ──────────────────────────────────────────────────────
# India 91-day T-Bill / repo rate (annualised decimal).  6.5 % → 0.065
RISK_FREE_RATE: float = 0.065

# Continuous dividend yield.  0 for pure indices (NIFTY, BANKNIFTY, SENSEX).
# For individual equity options that pay dividends, set e.g. 0.012 (1.2 %).
DIVIDEND_YIELD: float = 0.0

# Expiry contracts settle at 15:30 IST on the expiry date.
EXPIRY_HOUR, EXPIRY_MINUTE = 15, 30

# ── Method 2: Underlying spot price dictionary ────────────────────────────────
# Fill in today's (or any historical day's) closing spot for every underlying
# whose data you are processing.  Keys are UPPERCASE, no spaces.
# Leave empty ({}) if all your files include an Underlying_LTP column.
#
# Example:
#   UNDERLYING_PRICES = {
#       "NIFTY":     22150.75,
#       "BANKNIFTY": 47605.00,
#       "SENSEX":    73200.00,
#       "RELIANCE":  2983.50,
#   }
UNDERLYING_PRICES: dict[str, float] = {
    # "NIFTY":     22150.75,
    # "BANKNIFTY": 47605.00,
    # "SENSEX":    73200.00,
}

# ── IV solver bounds ──────────────────────────────────────────────────────────
IV_MIN: float = 1e-6    # 0.0001 %  – prevents divide-by-zero
IV_MAX: float = 20.000  # 2 000 %   – filter nonsensical solves

# Minimum TTE to avoid Black-Scholes singularities (1 hour in years)
MIN_TTE: float = 1.0 / (365.25 * 24)


# ══════════════════════════════════════════════════════════════════════════════
#  INPUT COLUMN NAME ALIASES  (case-insensitive matching applied at load time)
# ══════════════════════════════════════════════════════════════════════════════
# Map every known alias to an internal canonical name.  Add your own aliases
# here if your broker exports different headers.
_ALIAS_MAP: dict[str, str] = {
    # Timestamp
    "timestamp":         "Timestamp",
    "datetime":          "Timestamp",
    "date_time":         "Timestamp",
    "time":              "Timestamp",
    "received_at":       "Timestamp",

    # Symbol
    "symbol":            "Symbol",
    "tradingsymbol":     "Symbol",
    "contract":          "Symbol",
    "scrip":             "Symbol",

    # Option market price
    "ltp":               "LTP",
    "last_traded_price": "LTP",
    "lasttradedprice":   "LTP",
    "close":             "LTP",

    # Best bid / ask
    "best_bid_or_ask":   "Best_Bid_Or_Ask",
    "best_bid_ask":      "Best_Bid_Or_Ask",
    "bid_ask":           "Best_Bid_Or_Ask",
    "bid":               "Best_Bid_Or_Ask",

    # Open interest
    "oi":                "OI",
    "open_interest":     "OI",
    "openinterest":      "OI",

    # Underlying spot price (in-file method)
    "underlying_ltp":    "Underlying_LTP",
    "spot":              "Underlying_LTP",
    "spot_price":        "Underlying_LTP",
    "underlying":        "Underlying_LTP",
    "undrl_price":       "Underlying_LTP",
    "spot_ltp":          "Underlying_LTP",
}

# Required canonical columns that MUST be present after alias resolution
_REQUIRED_COLS = {"Timestamp", "Symbol", "LTP"}

# Output columns appended to every row (NaN where calculation not applicable)
_OUT_COLS = [
    "Parsed_Underlying",    # e.g. NIFTY
    "Parsed_Expiry",        # YYYY-MM-DD
    "Parsed_Strike",        # float
    "Parsed_OptionType",    # CE or PE
    "Spot_Used",            # underlying price actually fed into BS
    "Option_Price_Used",    # best_bid_or_ask → LTP fallback
    "TTE_Years",            # time-to-expiry in decimal years
    "IV",                   # implied volatility (decimal, e.g. 0.18)
    "IV_Pct",               # IV as percentage (18.00)
    "Delta",
    "Gamma",
    "Theta_Day",            # theta per calendar day ($/day)
    "Vega_1Pct",            # vega per 1 % IV move
    "Rho_1Pct",             # rho per 1 % rate move
    "Calc_Status",          # OK | IV_FAIL | UNDERLYING_MISSING | EXPIRED | PARSE_FAIL
]

# Timestamp format strings tried in order; first that parses ≥ 95 % of rows wins
_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",   # 2026-03-03 09:16:01.123   (primary spec)
    "%Y-%m-%dT%H:%M:%S.%f",   # ISO-8601 with microseconds
    "%Y-%m-%d %H:%M:%S",      # without fractional seconds
    "%Y-%m-%dT%H:%M:%S",      # ISO-8601 without microseconds
    "%d-%m-%Y %H:%M:%S.%f",   # Indian DD-MM-YYYY
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
]


# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("iv_enricher")


# ══════════════════════════════════════════════════════════════════════════════
#  OPTION SYMBOL PARSER
# ══════════════════════════════════════════════════════════════════════════════

# Month name → integer
_MON3 = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Single-character month code used in NSE weekly options (1–9, O, N, D)
_MNCHAR = {
    "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
    "6": 6, "7": 7, "8": 8, "9": 9,
    "O": 10, "N": 11, "D": 12,
}

# ── Regex patterns (compiled once at module load) ─────────────────────────────

# Format A  –  Sharekhan / NSE long monthly
#   NIFTY24MAR22100CE   →  underlying=NIFTY, day=24, mon=MAR, yr=22, K=2100, type=CE
#   BANKNIFTY27MAR2548000PE, RELIANCE31DEC251200CE
_RE_A = re.compile(
    r"""^
    (?P<und>[A-Z0-9&\-\.]+?)        # underlying  (non-greedy to avoid eating day)
    (?P<dd>\d{1,2})                 # expiry day
    (?P<mmm>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)
    (?P<yy>\d{2})                   # 2-digit year
    (?P<K>\d+(?:\.\d+)?)            # strike (allows decimal e.g. 2100.5)
    (?P<ot>CE|PE)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Format B  –  NSE compact weekly  YYMMDD
#   NIFTY2503137500CE  →  yr=25, mon=03, day=13, K=7500, type=CE
_RE_B = re.compile(
    r"""^
    (?P<und>[A-Z0-9&\-\.]+?)
    (?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})
    (?P<K>\d+(?:\.\d+)?)
    (?P<ot>CE|PE)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Format C  –  NSE compact weekly  YY + single letter month + DD
#   NIFTY253A137500CE  →  yr=25, mchar=3(Mar), day=13, K=7500, type=CE
_RE_C = re.compile(
    r"""^
    (?P<und>[A-Z0-9&\-\.]+?)
    (?P<yy>\d{2})(?P<mc>[1-9OND])(?P<dd>\d{2})
    (?P<K>\d+(?:\.\d+)?)
    (?P<ot>CE|PE)$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _expiry_date(dd: int, mm: int, yy: int) -> Optional[date]:
    """Build a date from (day, month, 2-digit-year); return None if invalid."""
    try:
        return date(2000 + yy, mm, dd)
    except ValueError:
        return None


def parse_symbol(raw: str) -> Optional[dict]:
    """
    Decompose an Indian F&O option symbol into its constituent parts.

    Tries three regex patterns in priority order:
      A → long monthly (NIFTY24MAR22100CE)
      B → compact YYMMDD weekly (NIFTY2503137500CE)
      C → compact YY+letter-month+DD (NIFTY253A137500CE)

    Returns
    -------
    dict with keys:
        underlying : str    e.g. "NIFTY"
        expiry     : date
        strike     : float
        flag       : str    "c" (call) or "p" (put) — py_vollib convention
    or None if the symbol does not match any pattern.
    """
    s = raw.strip().upper()

    # ── Pattern A ────────────────────────────────────────────────────────
    m = _RE_A.match(s)
    if m:
        exp = _expiry_date(int(m["dd"]), _MON3[m["mmm"].upper()], int(m["yy"]))
        if exp:
            return {
                "underlying": m["und"],
                "expiry":     exp,
                "strike":     float(m["K"]),
                "flag":       "c" if m["ot"].upper() == "CE" else "p",
            }

    # ── Pattern B ────────────────────────────────────────────────────────
    m = _RE_B.match(s)
    if m:
        exp = _expiry_date(int(m["dd"]), int(m["mm"]), int(m["yy"]))
        if exp:
            return {
                "underlying": m["und"],
                "expiry":     exp,
                "strike":     float(m["K"]),
                "flag":       "c" if m["ot"].upper() == "CE" else "p",
            }

    # ── Pattern C ────────────────────────────────────────────────────────
    m = _RE_C.match(s)
    if m:
        month = _MNCHAR.get(m["mc"].upper())
        if month:
            exp = _expiry_date(int(m["dd"]), month, int(m["yy"]))
            if exp:
                return {
                    "underlying": m["und"],
                    "expiry":     exp,
                    "strike":     float(m["K"]),
                    "flag":       "c" if m["ot"].upper() == "CE" else "p",
                }

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  CSV LOADING  —  alias normalisation, timestamp parsing, type coercion
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename DataFrame columns to canonical names using _ALIAS_MAP.
    Matching is case-insensitive and strips surrounding whitespace.
    """
    rename = {}
    for col in df.columns:
        canonical = _ALIAS_MAP.get(col.strip().lower())
        if canonical:
            rename[col] = canonical

    if rename:
        df = df.rename(columns=rename)

    return df


def _parse_timestamps(series: pd.Series) -> pd.Series:
    """
    Parse a string Series into datetime64 using multiple format candidates.

    For each format we attempt a strict parse.  The first format that
    converts ≥ 95 % of non-null values is adopted.  Any remaining failures
    fall to pd.to_datetime's mixed-format inference with errors='coerce'.

    Returns a Series of datetime64[ns] (NaT for unparseable entries).
    """
    non_null = series.dropna()
    if non_null.empty:
        return pd.to_datetime(series, errors="coerce")

    total = len(non_null)

    for fmt in _TS_FORMATS:
        try:
            candidate = pd.to_datetime(series, format=fmt, errors="coerce")
            hit_rate  = candidate.notna().sum() / total
            if hit_rate >= 0.95:
                return candidate
        except Exception:
            continue

    # Final fallback: let pandas infer (slower but handles mixed formats)
    return pd.to_datetime(series, infer_datetime_format=True, errors="coerce")


def load_csv(path: Path) -> Optional[pd.DataFrame]:
    """
    Read a CSV, normalise column names, parse timestamps, and coerce numeric
    columns.  Returns None if the file cannot be read or is empty.
    """
    try:
        df = pd.read_csv(path, low_memory=False, dtype=str)
    except Exception as exc:
        log.error("  Cannot read %s — %s", path.name, exc)
        return None

    if df.empty:
        log.warning("  File is empty: %s", path.name)
        return None

    # Strip whitespace from all column headers
    df.columns = [c.strip() for c in df.columns]

    # Normalise to canonical names
    df = _normalise_headers(df)

    # Validate required columns are present after normalisation
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        log.error(
            "  SKIP %s — required column(s) missing after alias resolution: %s\n"
            "  Available columns: %s",
            path.name, sorted(missing), sorted(df.columns),
        )
        return None

    # Parse timestamps in-place
    df["Timestamp"] = _parse_timestamps(df["Timestamp"])
    bad_ts = df["Timestamp"].isna().sum()
    if bad_ts:
        log.warning("  %d row(s) had unparseable timestamps — will be skipped.", bad_ts)

    # Coerce price / numeric columns to float
    for col in ("LTP", "Best_Bid_Or_Ask", "OI", "Underlying_LTP"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info("  Loaded %d rows, %d columns from %s", len(df), len(df.columns), path.name)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  UNDERLYING SPOT PRICE RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def resolve_spot(underlying: str, row: pd.Series) -> Optional[float]:
    """
    Determine the underlying spot price for a single row.

    Resolution order:
      1. "Underlying_LTP" column in the row (if column exists and value is valid)
      2. UNDERLYING_PRICES dict defined at the top of this script

    Returns a positive float or None.
    """
    # Method 1: in-file column
    col_val = row.get("Underlying_LTP")
    if col_val is not None and not (isinstance(col_val, float) and math.isnan(col_val)):
        try:
            v = float(col_val)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass

    # Method 2: user-defined dict (case-insensitive lookup)
    dict_val = UNDERLYING_PRICES.get(underlying.upper())
    if dict_val is not None and dict_val > 0:
        return float(dict_val)

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  BLACK-SCHOLES  —  IV SOLVER
# ══════════════════════════════════════════════════════════════════════════════

def solve_iv(
    option_price: float,
    S:            float,    # spot price
    K:            float,    # strike
    T:            float,    # time to expiry (years)
    r:            float,    # risk-free rate (decimal)
    flag:         str,      # "c" or "p"
    q:            float = DIVIDEND_YIELD,
) -> Optional[float]:
    """
    Solve for Black-Scholes Implied Volatility using py_vollib.

    The function defensively pre-validates the inputs before calling the
    py_vollib solver and handles all known failure modes:

      • option_price ≤ 0 or NaN             → return None
      • T ≤ MIN_TTE                          → return None (expired / too close)
      • price below theoretical intrinsic    → return None (no real IV exists)
      • solver convergence failure           → return None (py_vollib returns NaN)
      • IV outside [IV_MIN, IV_MAX]          → return None (nonsensical result)

    Parameters
    ----------
    option_price : float  – market price of the option (mid/LTP)
    S            : float  – underlying spot price
    K            : float  – strike price
    T            : float  – time to expiry in decimal years (must be > 0)
    r            : float  – annualised risk-free rate (e.g. 0.065)
    flag         : str    – "c" for call, "p" for put
    q            : float  – continuous dividend yield (default 0)

    Returns
    -------
    float  – IV as a decimal (e.g. 0.1823 for 18.23 %)
    None   – if IV cannot be determined from the given inputs
    """
    # ── Input guards ──────────────────────────────────────────────────────
    if not math.isfinite(option_price) or option_price <= 0:
        return None
    if not math.isfinite(S) or S <= 0:
        return None
    if not math.isfinite(K) or K <= 0:
        return None
    if T < MIN_TTE:
        return None

    # Intrinsic-value check: BS IV is mathematically undefined when the
    # option trades below its intrinsic value (arbitrage domain).
    intrinsic = max(0.0, (S - K) if flag == "c" else (K - S))
    if option_price < intrinsic * 0.999:
        # 0.1 % tolerance for rounding
        return None

    # ── py_vollib call ─────────────────────────────────────────────────────
    try:
        iv = _bsiv(
            discounted_option_price=option_price,
            S=S,
            K=K,
            t=T,
            r=r,
            flag=flag,
        )
    except Exception:
        # The solver raises when the bracketing completely fails
        return None

    # ── Post-solve validation ─────────────────────────────────────────────
    if iv is None or not math.isfinite(iv):
        return None
    if not (IV_MIN <= iv <= IV_MAX):
        return None

    return float(iv)


# ══════════════════════════════════════════════════════════════════════════════
#  BLACK-SCHOLES  —  ANALYTICAL GREEKS
# ══════════════════════════════════════════════════════════════════════════════

def compute_greeks(
    S:    float,
    K:    float,
    T:    float,
    r:    float,
    iv:   float,
    flag: str,
) -> dict:
    """
    Compute all five Black-Scholes Greeks using py_vollib's analytical module.

    py_vollib outputs are already normalised for practical use:
      • theta   : divided by 365  →  price decay per ONE calendar day
      • vega    : divided by 100  →  price change per 1 percentage-point rise in IV
      • rho     : divided by 100  →  price change per 1 percentage-point rise in r

    Parameters
    ----------
    S, K, T, r, iv : standard Black-Scholes inputs
    flag           : "c" for call, "p" for put

    Returns
    -------
    dict with keys: delta, gamma, theta_day, vega_1pct, rho_1pct
    Values are float; None if the calculation raises.

    Design note: we call each greek inside its own try/except so a failure in
    one does not prevent the others from being returned.
    """
    out = {
        "delta":     None,
        "gamma":     None,
        "theta_day": None,
        "vega_1pct": None,
        "rho_1pct":  None,
    }

    # A degenerate set of inputs cannot produce meaningful Greeks
    if T <= 0 or iv <= 0 or S <= 0 or K <= 0:
        return out

    def _safe(fn, key):
        try:
            val = float(fn(flag, S, K, T, r, iv))
            out[key] = val if math.isfinite(val) else None
        except Exception:
            pass   # leave as None

    _safe(_delta, "delta")
    _safe(_gamma, "gamma")
    _safe(_theta, "theta_day")
    _safe(_vega,  "vega_1pct")
    _safe(_rho,   "rho_1pct")

    return out


# ══════════════════════════════════════════════════════════════════════════════
#  VECTORISED SYMBOL PARSING  +  TTE CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def parse_symbols_to_df(symbols: pd.Series) -> pd.DataFrame:
    """
    Parse a full Series of trading symbols in one pass.

    Instead of calling parse_symbol() row-by-row inside the main loop
    (slow for large frames), we parse once here and store the results in a
    helper DataFrame that is passed into the row-by-row enrichment step.

    Returns a DataFrame with columns:
        _underlying, _expiry, _strike, _flag, _parse_ok
    Indexed identically to the input Series.
    """
    results = []
    for sym in symbols:
        p = parse_symbol(str(sym)) if pd.notna(sym) else None
        if p:
            results.append({
                "_underlying": p["underlying"],
                "_expiry":     p["expiry"],
                "_strike":     p["strike"],
                "_flag":       p["flag"],
                "_parse_ok":   True,
            })
        else:
            results.append({
                "_underlying": None,
                "_expiry":     None,
                "_strike":     None,
                "_flag":       None,
                "_parse_ok":   False,
            })

    meta = pd.DataFrame(results, index=symbols.index)
    ok   = meta["_parse_ok"].sum()
    log.info(
        "  Symbol parse: %d / %d succeeded (%.1f %%)",
        ok, len(symbols), 100 * ok / max(len(symbols), 1),
    )
    return meta


def compute_tte_series(
    timestamps: pd.Series,   # datetime64 Series
    expiries:   pd.Series,   # date objects  (or NaT/None)
) -> pd.Series:
    """
    Vectorised time-to-expiry calculation.

    For each aligned (timestamp, expiry_date) pair, build the expiry
    datetime at 15:30:00 on the expiry date and subtract the timestamp.
    Store the result in decimal years (float64 Series).

    Expired or unparseable entries are stored as NaN.
    """
    expiry_dts = pd.to_datetime(
        [
            datetime(e.year, e.month, e.day, EXPIRY_HOUR, EXPIRY_MINUTE)
            if isinstance(e, date) and not isinstance(e, datetime)
            else pd.NaT
            for e in expiries
        ]
    )

    # timedelta → total seconds → years
    diff_sec  = (expiry_dts - timestamps).dt.total_seconds()
    tte_years = diff_sec / (365.25 * 86400)

    # Cap at MIN_TTE; flag expired rows with NaN
    tte_years = tte_years.where(tte_years >= MIN_TTE, other=np.nan)

    return tte_years


# ══════════════════════════════════════════════════════════════════════════════
#  ROW-BY-ROW ENRICHMENT  (IV solver is inherently iterative, cannot vectorise)
# ══════════════════════════════════════════════════════════════════════════════

def enrich_dataframe(df: pd.DataFrame, risk_free: float) -> pd.DataFrame:
    """
    Enrich a loaded options DataFrame with IV and Greeks columns.

    Pipeline:
      1.  Vectorised: parse all symbols → underlying, expiry, strike, flag
      2.  Vectorised: compute TTE for every row using pandas datetime arithmetic
      3.  Row-by-row: resolve spot price → call IV solver → call Greeks
          (these three steps are inherently serial due to the Newton-Raphson solver)

    Parameters
    ----------
    df          : normalised DataFrame from load_csv()
    risk_free   : annualised risk-free rate (decimal)

    Returns
    -------
    The input DataFrame with _OUT_COLS appended.
    Any row that cannot be fully enriched gets NaN in Greek columns and a
    descriptive string in Calc_Status.
    """
    n_rows = len(df)

    # ── Step 1: Vectorised symbol parsing ────────────────────────────────
    log.info("  Parsing %d symbols …", n_rows)
    meta = parse_symbols_to_df(df["Symbol"])

    # ── Step 2: Vectorised TTE ────────────────────────────────────────────
    log.info("  Computing time-to-expiry …")
    tte_series = compute_tte_series(df["Timestamp"], meta["_expiry"])

    # ── Step 3: Pre-allocate output columns with NaN ──────────────────────
    for col in _OUT_COLS:
        df[col] = np.nan
    df["Calc_Status"] = "PENDING"    # will be overwritten per-row

    # ── Step 4: Row-by-row Black-Scholes ──────────────────────────────────
    log.info("  Running IV + Greeks for %d rows …", n_rows)

    status_counts: dict[str, int] = {}

    for idx in df.index:
        # ── 4a. Symbol parse result ───────────────────────────────────────
        if not meta.at[idx, "_parse_ok"]:
            df.at[idx, "Calc_Status"] = "PARSE_FAIL"
            status_counts["PARSE_FAIL"] = status_counts.get("PARSE_FAIL", 0) + 1
            continue

        underlying  = meta.at[idx, "_underlying"]
        expiry_date = meta.at[idx, "_expiry"]
        strike      = meta.at[idx, "_strike"]
        flag        = meta.at[idx, "_flag"]
        option_type = "CE" if flag == "c" else "PE"

        # Write parsed symbol components unconditionally (useful for debugging)
        df.at[idx, "Parsed_Underlying"] = underlying
        df.at[idx, "Parsed_Expiry"]     = expiry_date.isoformat()
        df.at[idx, "Parsed_Strike"]     = strike
        df.at[idx, "Parsed_OptionType"] = option_type

        # ── 4b. Timestamp validity ────────────────────────────────────────
        ts = df.at[idx, "Timestamp"]
        if pd.isna(ts):
            df.at[idx, "Calc_Status"] = "PARSE_FAIL"
            status_counts["PARSE_FAIL"] = status_counts.get("PARSE_FAIL", 0) + 1
            continue

        # ── 4c. TTE ───────────────────────────────────────────────────────
        tte = tte_series.at[idx]
        if not math.isfinite(tte) or tte < MIN_TTE:
            df.at[idx, "Calc_Status"] = "EXPIRED"
            status_counts["EXPIRED"] = status_counts.get("EXPIRED", 0) + 1
            continue

        df.at[idx, "TTE_Years"] = round(tte, 8)

        # ── 4d. Underlying spot price ─────────────────────────────────────
        spot = resolve_spot(underlying, df.loc[idx])
        if spot is None:
            df.at[idx, "Calc_Status"] = "UNDERLYING_MISSING"
            status_counts["UNDERLYING_MISSING"] = (
                status_counts.get("UNDERLYING_MISSING", 0) + 1
            )
            continue

        df.at[idx, "Spot_Used"] = round(spot, 4)

        # ── 4e. Option market price (Best_Bid_Or_Ask → LTP fallback) ──────
        opt_price: Optional[float] = None

        bba = df.at[idx, "Best_Bid_Or_Ask"] if "Best_Bid_Or_Ask" in df.columns else np.nan
        if bba is not None and not (isinstance(bba, float) and math.isnan(bba)) and float(bba) > 0:
            opt_price = float(bba)
        else:
            ltp = df.at[idx, "LTP"]
            if ltp is not None and not (isinstance(ltp, float) and math.isnan(ltp)) and float(ltp) > 0:
                opt_price = float(ltp)

        if opt_price is None:
            df.at[idx, "Calc_Status"] = "IV_FAIL"
            status_counts["IV_FAIL"] = status_counts.get("IV_FAIL", 0) + 1
            continue

        df.at[idx, "Option_Price_Used"] = round(opt_price, 4)

        # ── 4f. Implied Volatility ────────────────────────────────────────
        iv = solve_iv(
            option_price=opt_price,
            S=spot,
            K=strike,
            T=tte,
            r=risk_free,
            flag=flag,
        )

        if iv is None:
            df.at[idx, "Calc_Status"] = "IV_FAIL"
            status_counts["IV_FAIL"] = status_counts.get("IV_FAIL", 0) + 1
            continue

        df.at[idx, "IV"]     = round(iv, 6)
        df.at[idx, "IV_Pct"] = round(iv * 100.0, 4)

        # ── 4g. Greeks ────────────────────────────────────────────────────
        g = compute_greeks(S=spot, K=strike, T=tte, r=risk_free, iv=iv, flag=flag)

        df.at[idx, "Delta"]     = _r(g["delta"],     6)
        df.at[idx, "Gamma"]     = _r(g["gamma"],     8)
        df.at[idx, "Theta_Day"] = _r(g["theta_day"], 4)
        df.at[idx, "Vega_1Pct"] = _r(g["vega_1pct"], 4)
        df.at[idx, "Rho_1Pct"]  = _r(g["rho_1pct"],  4)

        df.at[idx, "Calc_Status"] = "OK"
        status_counts["OK"] = status_counts.get("OK", 0) + 1

    # ── Final status summary ───────────────────────────────────────────────
    log.info("  Status breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        log.info("    %-25s : %d", status, count)

    return df


def _r(val: Optional[float], decimals: int) -> Optional[float]:
    """Round a float to `decimals` places, passthrough None unchanged."""
    if val is None:
        return None
    return round(val, decimals)


# ══════════════════════════════════════════════════════════════════════════════
#  FILE PROCESSOR
# ══════════════════════════════════════════════════════════════════════════════

def process_file(
    src:        Path,
    output_dir: Path,
    risk_free:  float,
) -> dict:
    """
    Full pipeline for a single CSV file.

    Returns a stats dict with: rows_in, rows_ok, rows_fail, elapsed_s
    """
    t0 = time.perf_counter()
    stats = {"rows_in": 0, "rows_ok": 0, "rows_fail": 0, "elapsed_s": 0.0}

    log.info("─" * 62)
    log.info("FILE  : %s", src.name)
    log.info("─" * 62)

    # Load and normalise
    df = load_csv(src)
    if df is None:
        return stats

    stats["rows_in"] = len(df)

    # Enrich
    df = enrich_dataframe(df, risk_free)

    # Tabulate success/failure
    ok   = (df["Calc_Status"] == "OK").sum()
    fail = len(df) - ok
    stats["rows_ok"]   = int(ok)
    stats["rows_fail"] = int(fail)
    ok_pct = 100 * ok / max(len(df), 1)
    log.info("  Enriched OK   : %d / %d  (%.1f %%)", ok, len(df), ok_pct)

    # Write enriched CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{src.stem}{OUTPUT_SUFFIX}.csv"
    try:
        # utf-8-sig includes the BOM so Excel opens the file correctly
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        log.info("  Output        : %s", out_path)
    except OSError as exc:
        log.error("  Write failed  : %s", exc)

    stats["elapsed_s"] = round(time.perf_counter() - t0, 2)
    log.info("  Elapsed       : %.2f s", stats["elapsed_s"])
    return stats


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _gather_files(path: Path) -> list[Path]:
    """Return sorted list of CSVs; accepts a file or directory."""
    if path.is_file():
        return [path] if path.suffix.lower() == ".csv" else []
    if path.is_dir():
        return sorted(path.glob("*.csv"))
    return []


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Post-market IV & Greeks enricher for Indian F&O option CSVs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python iv_greeks_enricher.py
  python iv_greeks_enricher.py --input data\\options_today.csv
  python iv_greeks_enricher.py --input data\\ --output results\\ --rate 0.068
  python iv_greeks_enricher.py --verbose
""",
    )
    ap.add_argument("--input",   "-i", type=Path, default=DEFAULT_INPUT,
                    metavar="PATH", help="CSV file or directory of CSVs to process")
    ap.add_argument("--output",  "-o", type=Path, default=DEFAULT_OUTPUT,
                    metavar="DIR",  help="Directory for enriched output files")
    ap.add_argument("--rate",    "-r", type=float, default=RISK_FREE_RATE,
                    metavar="R",    help="Annualised risk-free rate  (e.g. 0.065 = 6.5%)")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Enable DEBUG logging")
    args = ap.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 62)
    log.info("  IV & Greeks Enricher  —  post-market / local desktop")
    log.info("═" * 62)
    log.info("  Input          : %s", args.input)
    log.info("  Output         : %s", args.output)
    log.info("  Risk-free rate : %.4f  (%.2f %%)", args.rate, args.rate * 100)
    log.info("  Dividend yield : %.4f", DIVIDEND_YIELD)
    log.info("  Underlying dict: %d entry(s)", len(UNDERLYING_PRICES))

    # Collect input files
    files = _gather_files(args.input)
    if not files:
        log.critical(
            "No CSV file(s) found at: %s\n"
            "  Pass --input pointing to a .csv file or a directory of .csv files.",
            args.input,
        )
        raise SystemExit(1)
    log.info("  Files to process: %d\n", len(files))

    # Process each file
    wall_t0    = time.perf_counter()
    grand_in   = 0
    grand_ok   = 0
    grand_fail = 0

    for i, fp in enumerate(files, 1):
        log.info("[%d/%d]", i, len(files))
        s = process_file(fp, args.output, args.rate)
        grand_in   += s["rows_in"]
        grand_ok   += s["rows_ok"]
        grand_fail += s["rows_fail"]

    # Summary
    wall_s = round(time.perf_counter() - wall_t0, 2)
    ok_pct = 100 * grand_ok / max(grand_in, 1)
    log.info("\n" + "═" * 62)
    log.info("  SUMMARY")
    log.info("  Files   : %d", len(files))
    log.info("  Rows in : %d", grand_in)
    log.info("  OK      : %d  (%.1f %%)", grand_ok,   ok_pct)
    log.info("  Failed  : %d  (%.1f %%)", grand_fail, 100 - ok_pct)
    log.info("  Time    : %.2f s", wall_s)
    log.info("  Output  : %s", args.output.resolve())
    log.info("═" * 62)


if __name__ == "__main__":
    main()
