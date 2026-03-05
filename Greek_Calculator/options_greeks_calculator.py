#!/usr/bin/env python3
"""
options_greeks_calculator.py
============================
Post-market, local Windows PC enrichment script.

Purpose
-------
Reads every raw tick/minute CSV file produced by the Sharekhan tick harvester
(or any compatible file), resolves each option row's Implied Volatility (IV)
via the Black-Scholes model, then appends the four main Greeks —
Delta, Gamma, Theta (daily), Vega (per 1 vol %) — plus metadata columns
to a new output CSV.

Design goals
-----------
  ✔  Runs 100 % locally — exploits your desktop CPU (multi-file loop)
  ✔  Pandas for fast columnar operations on large day-files
  ✔  py_vollib for all Black-Scholes calculations (lightweight C extension)
  ✔  Full multi-format symbol parser handles Sharekhan and NSE short codes
  ✔  Robust IV solver with calibrated bounds and graceful fallback
  ✔  Detailed per-file progress console output and a run-summary at the end

Usage
-----
  python options_greeks_calculator.py                         # process tick_data\\ folder
  python options_greeks_calculator.py --input  my_data\\      # custom input folder
  python options_greeks_calculator.py --input  single.csv     # single file
  python options_greeks_calculator.py --output results\\      # custom output folder
  python options_greeks_calculator.py --rate 0.068            # override risk-free rate

Dependencies
------------
  pip install py_vollib pandas numpy scipy
  (see greeks_requirements.txt for pinned versions)
"""

# ─────────────────────────────────────────────────────────────────────────────
#  Standard-library imports
# ─────────────────────────────────────────────────────────────────────────────
import argparse
import csv
import logging
import math
import re
import sys
import time
import warnings
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
#  Third-party imports  (install: pip install py_vollib pandas numpy scipy)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import pandas as pd
    import numpy as np
except ImportError:
    sys.exit(
        "ERROR: 'pandas' and 'numpy' not found.\n"
        "       Run:  pip install pandas numpy\n"
    )

try:
    from py_vollib.black_scholes.implied_volatility import implied_volatility as _bs_iv
    from py_vollib.black_scholes.greeks.analytical import (
        delta as _bs_delta,
        gamma as _bs_gamma,
        theta as _bs_theta,   # py_vollib already returns theta per CALENDAR DAY
        vega  as _bs_vega,    # py_vollib already returns vega per 1% IV change
        rho   as _bs_rho,     # py_vollib already returns rho per 1% rate change
    )
except ImportError:
    sys.exit(
        "ERROR: 'py_vollib' not found.\n"
        "       Run:  pip install py_vollib\n"
    )


# ═════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  ← edit these before running
# ═════════════════════════════════════════════════════════════════════════════

# ── Paths ────────────────────────────────────────────────────────────────────
_BASE_DIR    = Path(__file__).parent
_ROOT_DIR    = _BASE_DIR.parent                                   # Stock-Predictor-v1.1.0
DEFAULT_INPUT_DIR  = _ROOT_DIR / "Tick_Data_Folder" / "tick_data"
DEFAULT_OUTPUT_DIR = _ROOT_DIR / "greeks_output"

# ── Financial parameters ─────────────────────────────────────────────────────
# India 91-day T-Bill yield as of early 2026 (annualised decimal).
# Override with --rate on the command line.
RISK_FREE_RATE: float = 0.065   # 6.5 %

# Dividend yield: 0 for pure indices (NIFTY, BANKNIFTY, SENSEX).
# Set a small value (e.g. 0.01) for individual stock options if needed.
DIVIDEND_YIELD: float = 0.0

# Indian F&O expiry time: 15:30 IST
EXPIRY_HOUR   = 15
EXPIRY_MINUTE = 30

# Minimum TTE floor to avoid near-zero singularities (1 minute in years)
MIN_TTE_YEARS: float = 1.0 / (365.25 * 24 * 60)

# IV solver search range (as annualised decimal, e.g. 0.01 = 1%, 20.0 = 2000%)
IV_LOWER_BOUND: float = 1e-4
IV_UPPER_BOUND: float = 20.0

# ── Column name mapping (harvester CSV schema) ───────────────────────────────
# If your CSV uses different headers, update these constants.
COL_TIMESTAMP   = "received_at"      # ISO-8601 datetime string with ±ms
COL_SYMBOL      = "symbol"           # full trading symbol, e.g. NIFTY27MAR2524000CE
COL_EXCHANGE    = "exchange"         # e.g. NC (NSE Cash), NF (NSE F&O)
COL_LTP         = "ltp"              # Last Traded Price of this instrument
COL_BID_ASK     = "best_bid_or_ask"  # optional; used as option price if present
COL_OI          = "oi"               # Open Interest

# ── Exchanges considered "spot/underlying" (not options themselves) ───────────
SPOT_EXCHANGES: frozenset[str] = frozenset({"NC", "BC", "NC ", "BC "})

# ── Output suffix appended to each enriched file name ────────────────────────
OUTPUT_SUFFIX = "_greeks"

# ── New columns appended to every option row ─────────────────────────────────
GREEKS_COLS = [
    "underlying_sym",     # resolved underlying name, e.g. NIFTY
    "expiry_date",        # YYYY-MM-DD
    "strike",             # strike price as float
    "option_type",        # CE or PE
    "underlying_ltp",     # spot price used for BS calculation
    "option_price_used",  # market price fed to the IV solver
    "tte_years",          # time to expiry in decimal years
    "iv",                 # implied volatility (decimal, e.g. 0.18 = 18%)
    "iv_pct",             # IV as percentage (18.0 for 18%)
    "delta",              # rate of change w.r.t. underlying price
    "gamma",              # rate of change of delta (per unit move in S)
    "theta_daily",        # time decay per calendar day (py_vollib normalised)
    "vega_1pct",          # price change per 1% rise in IV (py_vollib normalised)
    "rho_1pct",           # price change per 1% rise in r (py_vollib normalised)
    "calc_status",        # OK | IV_FAIL | NO_UNDERLYING | EXPIRED | PARSE_FAIL
]


# ═════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("greeks_calc")
warnings.filterwarnings("ignore")   # suppress py_vollib NaN warnings during IV search


# ═════════════════════════════════════════════════════════════════════════════
#  OPTION SYMBOL PARSER
# ═════════════════════════════════════════════════════════════════════════════

# Month abbreviation → integer mapping
_MONTH_MAP: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# NSE compact weekly option: month encoded as single character
# 1-9 = Jan-Sep ; O = Oct ; N = Nov ; D = Dec
_NSE_MONTH_CHAR: dict[str, int] = {
    "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
    "6": 6, "7": 7, "8": 8, "9": 9,
    "O": 10, "N": 11, "D": 12,
}

# ── Regex patterns (tried in priority order) ─────────────────────────────────

# Pattern 1 — Sharekhan / NNE long monthly-expiry format
#   NIFTY27MAR2524000CE  →  underlying=NIFTY, day=27, mon=MAR, yr=25, K=24000, type=CE
#   RELIANCE27MAR252500CE, BANKNIFTY27MAR2548000PE
_RE_LONG = re.compile(
    r"""^
    (?P<underlying>[A-Z0-9&\-\.]+?)     # underlying name (non-greedy)
    (?P<day>\d{1,2})                    # expiry day
    (?P<mon>JAN|FEB|MAR|APR|MAY|JUN|
            JUL|AUG|SEP|OCT|NOV|DEC)   # expiry month  (3-letter)
    (?P<yr>\d{2})                       # expiry year   (2-digit)
    (?P<strike>\d+(?:\.\d+)?)           # strike price
    (?P<flag>CE|PE)$                    # option type
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Pattern 2 — NSE compact weekly: YYMMDD
#   NIFTY2502137500CE → yr=25, mon=02, day=13, K=7500, type=CE
#   This pattern produces a 6-digit date block — we validate the date after match.
_RE_COMPACT_YYMMDD = re.compile(
    r"""^
    (?P<underlying>[A-Z0-9&\-\.]+?)
    (?P<yr>\d{2})
    (?P<mon>\d{2})
    (?P<day>\d{2})
    (?P<strike>\d+(?:\.\d+)?)
    (?P<flag>CE|PE)$
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Pattern 3 — NSE compact weekly: YY + single-letter month code + DD
#   NIFTY25213700CE  → yr=25, mon_char=2(Feb), day=13, K=3700, type=CE
_RE_COMPACT_LETTER = re.compile(
    r"""^
    (?P<underlying>[A-Z0-9&\-\.]+?)
    (?P<yr>\d{2})
    (?P<mchar>[1-9OND])
    (?P<day>\d{2})
    (?P<strike>\d+(?:\.\d+)?)
    (?P<flag>CE|PE)$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _make_expiry(day: int, month: int, year_2d: int) -> Optional[date]:
    """
    Construct a date from day / month / 2-digit year.
    Returns None if the date is invalid.
    Year pivot: 25 → 2025, 99 → 2099.  Safe for options up to year 2099.
    """
    if year_2d < 0:
        return None
    full_year = 2000 + year_2d
    try:
        return date(full_year, month, day)
    except ValueError:
        return None


def parse_option_symbol(symbol: str) -> Optional[dict]:
    """
    Parse an Indian F&O option trading symbol into its component parts.

    Returns a dict with keys:
        underlying : str      e.g. "NIFTY"
        expiry_date: date
        strike     : float
        flag       : str      "c" (call) or "p" (put)  — py_vollib convention

    Returns None if the symbol is not recognisable as an option contract.

    Supported formats (tried in order):
      1. NIFTY27MAR2524000CE    — Sharekhan / long monthly
      2. NIFTY2502137500CE      — NSE compact YYMMDD
      3. NIFTY25213700CE        — NSE compact YY + letter-month + DD
    """
    s = symbol.strip().upper()

    # ── Format 1: long month name ───────────────────────────────────────
    m = _RE_LONG.match(s)
    if m:
        day    = int(m.group("day"))
        month  = _MONTH_MAP[m.group("mon").upper()]
        yr     = int(m.group("yr"))
        expiry = _make_expiry(day, month, yr)
        if expiry is not None:
            return {
                "underlying":  m.group("underlying"),
                "expiry_date": expiry,
                "strike":      float(m.group("strike")),
                "flag":        "c" if m.group("flag").upper() == "CE" else "p",
            }

    # ── Format 2: YYMMDD compact ────────────────────────────────────────
    m = _RE_COMPACT_YYMMDD.match(s)
    if m:
        yr     = int(m.group("yr"))
        month  = int(m.group("mon"))
        day    = int(m.group("day"))
        expiry = _make_expiry(day, month, yr)
        if expiry is not None:
            return {
                "underlying":  m.group("underlying"),
                "expiry_date": expiry,
                "strike":      float(m.group("strike")),
                "flag":        "c" if m.group("flag").upper() == "CE" else "p",
            }

    # ── Format 3: YY + single letter month code + DD ────────────────────
    m = _RE_COMPACT_LETTER.match(s)
    if m:
        yr     = int(m.group("yr"))
        month  = _NSE_MONTH_CHAR.get(m.group("mchar").upper())
        day    = int(m.group("day"))
        if month is not None:
            expiry = _make_expiry(day, month, yr)
            if expiry is not None:
                return {
                    "underlying":  m.group("underlying"),
                    "expiry_date": expiry,
                    "strike":      float(m.group("strike")),
                    "flag":        "c" if m.group("flag").upper() == "CE" else "p",
                }

    return None     # not an option symbol we recognise


# ═════════════════════════════════════════════════════════════════════════════
#  UNDERLYING PRICE LOOKUP  (minute-bucket resolution)
# ═════════════════════════════════════════════════════════════════════════════

def build_underlying_price_index(df: pd.DataFrame) -> dict:
    """
    From a full-day tick DataFrame, build a nested dict:

        { underlying_name_upper : { minute_bucket_datetime : spot_ltp } }

    "Minute bucket" = received_at truncated to the minute (seconds zeroed).
    This lets us look up the nearest spot price for each option tick.

    Underlying rows are identified by SPOT_EXCHANGES (NC / BC) combined with
    a symbol that does NOT parse as an option contract.

    Parameters
    ----------
    df : DataFrame with at minimum COL_TIMESTAMP, COL_EXCHANGE, COL_SYMBOL, COL_LTP

    Returns
    -------
    dict[str, dict[datetime, float]]
    """
    price_index: dict[str, dict] = {}

    spot_mask = df[COL_EXCHANGE].str.strip().str.upper().isin(SPOT_EXCHANGES)
    spot_rows = df[spot_mask]

    if spot_rows.empty:
        log.warning(
            "  No spot-price rows found (exchange in %s). "
            "Option Greeks will have no underlying price.",
            sorted(SPOT_EXCHANGES),
        )
        return price_index

    for _, row in spot_rows.iterrows():
        sym = str(row[COL_SYMBOL]).strip().upper()

        # Skip rows whose symbol looks like an option (shouldn't happen for NC
        # but be defensive)
        if parse_option_symbol(sym) is not None:
            continue

        ltp_val = _safe_float(row[COL_LTP])
        if ltp_val is None or ltp_val <= 0:
            continue

        # Minute-bucket the timestamp
        ts = row.get(COL_TIMESTAMP)
        if pd.isna(ts) or not isinstance(ts, datetime):
            continue
        bucket = ts.replace(second=0, microsecond=0)

        if sym not in price_index:
            price_index[sym] = {}
        price_index[sym][bucket] = ltp_val

    log.info(
        "  Underlying index built: %d symbol(s),  total minute-buckets: %d",
        len(price_index),
        sum(len(v) for v in price_index.values()),
    )
    return price_index


def lookup_underlying_price(
    price_index: dict,
    underlying: str,
    tick_dt:     datetime,
    window_minutes: int = 5,
) -> Optional[float]:
    """
    Return the spot LTP for `underlying` closest to `tick_dt`.

    Search strategy:
      1.  Try the exact minute bucket.
      2.  Walk ±window_minutes minutes until a match is found.
      3.  Return None if no match within the window.

    Parameters
    ----------
    price_index    : output of build_underlying_price_index()
    underlying     : name in UPPER case, e.g. "NIFTY"
    tick_dt        : the datetime of the option tick
    window_minutes : maximum search radius in minutes
    """
    buckets = price_index.get(underlying)
    if not buckets:
        return None

    # Exact or nearest-minute search
    base = tick_dt.replace(second=0, microsecond=0)
    for offset in range(window_minutes + 1):
        for sign in (0, 1, -1):
            probe = base + timedelta(minutes=offset * sign)
            if probe in buckets:
                return buckets[probe]
            if sign == 0:
                break         # only probe exact once

    return None


# ═════════════════════════════════════════════════════════════════════════════
#  BLACK-SCHOLES IV SOLVER
# ═════════════════════════════════════════════════════════════════════════════

def calc_iv(
    option_price: float,
    S:            float,
    K:            float,
    T:            float,
    r:            float,
    flag:         str,
    q:            float = DIVIDEND_YIELD,
) -> Optional[float]:
    """
    Solve for the Black-Scholes Implied Volatility using py_vollib.

    Parameters
    ----------
    option_price : market price of the option (ideally best-bid-or-ask midpoint)
    S            : underlying spot price
    K            : option strike price
    T            : time to expiry in decimal years  (must be > 0)
    r            : risk-free rate (annualised decimal, e.g. 0.065)
    flag         : "c" for call, "p" for put
    q            : continuous dividend yield (0 for pure indices)

    Returns
    -------
    IV as a decimal (e.g. 0.1800 for 18.00 %) or None if the solver fails.

    Failure modes handled:
      • option price ≤ 0 or NaN
      • TTE ≤ 0 (expired contract)
      • Option price below intrinsic value (IV undefined)
      • Solver does not converge within bounds
    """
    if option_price is None or math.isnan(option_price) or option_price <= 0:
        return None
    if T <= 0:
        return None
    if S <= 0 or K <= 0:
        return None

    # Intrinsic value check — BS IV is undefined below these floors
    intrinsic = max(0.0, (S - K) if flag == "c" else (K - S))
    if option_price < intrinsic * 0.99:
        return None

    try:
        iv_val = _bs_iv(
            discounted_option_price=option_price,
            S=S,
            K=K,
            t=T,
            r=r,
            flag=flag,
        )
        # py_vollib returns NaN when the solver diverges
        if iv_val is None or math.isnan(iv_val) or iv_val <= 0:
            return None
        # Sanity-check: IV above 2000 % is almost certainly a bad solve
        if iv_val > IV_UPPER_BOUND:
            return None
        return float(iv_val)

    except Exception:
        # Swallow convergence / bounds errors — they are expected for
        # deep ITM/OTM options or stale prices
        return None


# ═════════════════════════════════════════════════════════════════════════════
#  BLACK-SCHOLES GREEKS
# ═════════════════════════════════════════════════════════════════════════════

def calc_greeks(
    S:    float,
    K:    float,
    T:    float,
    r:    float,
    iv:   float,
    flag: str,
) -> dict:
    """
    Calculate all five Black-Scholes Greeks using py_vollib's analytical module.

    py_vollib normalisation applied internally:
      • theta  : already divided by 365 → returned as  $/calendar_day
      • vega   : already divided by 100 → returned as  $/1% change in IV
      • rho    : already divided by 100 → returned as  $/1% change in r

    Parameters
    ----------
    S, K, T, r, iv : standard BS inputs
    flag           : "c" or "p"

    Returns
    -------
    dict with keys: delta, gamma, theta_daily, vega_1pct, rho_1pct
    All values are float or None if the calculation fails.
    """
    result = {
        "delta":       None,
        "gamma":       None,
        "theta_daily": None,
        "vega_1pct":   None,
        "rho_1pct":    None,
    }

    if T <= 0 or iv <= 0 or S <= 0 or K <= 0:
        return result

    try:
        result["delta"]       = float(_bs_delta(flag, S, K, T, r, iv))
        result["gamma"]       = float(_bs_gamma(flag, S, K, T, r, iv))
        result["theta_daily"] = float(_bs_theta(flag, S, K, T, r, iv))
        result["vega_1pct"]   = float(_bs_vega (flag, S, K, T, r, iv))
        result["rho_1pct"]    = float(_bs_rho  (flag, S, K, T, r, iv))

        # Guard against NaNs / Infs that can appear for extreme parameters
        for key, val in result.items():
            if val is not None and (math.isnan(val) or math.isinf(val)):
                result[key] = None

    except Exception:
        pass    # return whatever was computed before the failure

    return result


# ═════════════════════════════════════════════════════════════════════════════
#  UTILITY
# ═════════════════════════════════════════════════════════════════════════════

def _safe_float(value) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    try:
        f = float(value)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _option_price_for_row(row: pd.Series) -> Optional[float]:
    """
    Determine the market price to use as input for the IV solver.

    Priority:
      1. best_bid_or_ask column (if present and valid)
      2. ltp column (fallback)
    """
    # Check for optional Best_Bid_Or_Ask column (case-insensitive)
    for col in (COL_BID_ASK, "Best_Bid_Or_Ask", "best_bid_ask", "bid_ask"):
        if col in row.index:
            val = _safe_float(row[col])
            if val is not None and val > 0:
                return val

    return _safe_float(row.get(COL_LTP))


# ═════════════════════════════════════════════════════════════════════════════
#  ROW-LEVEL ENRICHMENT
# ═════════════════════════════════════════════════════════════════════════════

def enrich_row(
    row:         pd.Series,
    price_index: dict,
    risk_free:   float,
) -> dict:
    """
    Compute Greeks for a single option DataFrame row.

    Returns a dict with the keys defined in GREEKS_COLS.
    All fields are populated; calc_status describes the outcome.
    """
    empty = {col: np.nan for col in GREEKS_COLS}
    empty["calc_status"] = "SKIP"

    # ── 1. Parse the trading symbol ────────────────────────────────────────
    symbol     = str(row.get(COL_SYMBOL, "")).strip().upper()
    parsed     = parse_option_symbol(symbol)

    if parsed is None:
        empty["calc_status"] = "PARSE_FAIL"
        return empty

    underlying    = parsed["underlying"]
    expiry_dt     = parsed["expiry_date"]
    strike        = parsed["strike"]
    flag          = parsed["flag"]
    option_type   = "CE" if flag == "c" else "PE"

    empty.update({
        "underlying_sym": underlying,
        "expiry_date":    expiry_dt.isoformat(),
        "strike":         strike,
        "option_type":    option_type,
    })

    # ── 2. Parse the tick timestamp ────────────────────────────────────────
    ts_raw = row.get(COL_TIMESTAMP)
    if pd.isna(ts_raw) or not isinstance(ts_raw, datetime):
        empty["calc_status"] = "PARSE_FAIL"
        return empty

    tick_dt: datetime = ts_raw

    # ── 3. Compute time to expiry ──────────────────────────────────────────
    # Expiry is defined as the contract's expiry date at 15:30 IST.
    # Since the harvester stores timestamps in local/IST context, we work
    # in naive datetimes consistently.
    expiry_naive = datetime(
        expiry_dt.year, expiry_dt.month, expiry_dt.day,
        EXPIRY_HOUR, EXPIRY_MINUTE, 0,
    )

    tte_seconds = (expiry_naive - tick_dt).total_seconds()
    if tte_seconds <= 0:
        empty["calc_status"] = "EXPIRED"
        return empty

    tte_years = max(tte_seconds / (365.25 * 86400), MIN_TTE_YEARS)
    empty["tte_years"] = round(tte_years, 8)

    # ── 4. Resolve underlying spot price ──────────────────────────────────
    spot = lookup_underlying_price(price_index, underlying, tick_dt)
    if spot is None or spot <= 0:
        empty["calc_status"] = "NO_UNDERLYING"
        return empty

    empty["underlying_ltp"] = round(spot, 4)

    # ── 5. Determine option market price for IV solver ────────────────────
    opt_price = _option_price_for_row(row)
    if opt_price is None or opt_price <= 0:
        empty["calc_status"] = "IV_FAIL"
        return empty

    empty["option_price_used"] = round(opt_price, 4)

    # ── 6. Solve Implied Volatility ────────────────────────────────────────
    iv = calc_iv(
        option_price=opt_price,
        S=spot,
        K=strike,
        T=tte_years,
        r=risk_free,
        flag=flag,
    )

    if iv is None:
        empty["calc_status"] = "IV_FAIL"
        return empty

    empty["iv"]     = round(iv, 6)
    empty["iv_pct"] = round(iv * 100.0, 4)

    # ── 7. Calculate Greeks ────────────────────────────────────────────────
    greeks = calc_greeks(S=spot, K=strike, T=tte_years, r=risk_free, iv=iv, flag=flag)

    empty["delta"]       = _fmt(greeks["delta"],       6)
    empty["gamma"]       = _fmt(greeks["gamma"],       8)
    empty["theta_daily"] = _fmt(greeks["theta_daily"], 4)
    empty["vega_1pct"]   = _fmt(greeks["vega_1pct"],   4)
    empty["rho_1pct"]    = _fmt(greeks["rho_1pct"],    4)

    empty["calc_status"] = "OK"
    return empty


def _fmt(val: Optional[float], decimals: int) -> Optional[float]:
    """Round a float to `decimals` places, or return None."""
    if val is None:
        return None
    return round(val, decimals)


# ═════════════════════════════════════════════════════════════════════════════
#  FILE PROCESSOR
# ═════════════════════════════════════════════════════════════════════════════

_TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f",    # ISO-8601 with microseconds (harvester default)
    "%Y-%m-%dT%H:%M:%S",       # ISO-8601 without microseconds
    "%Y-%m-%d %H:%M:%S.%f",    # space-separated with microseconds
    "%Y-%m-%d %H:%M:%S",       # space-separated without microseconds
    "%d-%m-%Y %H:%M:%S",       # Indian date-first format
    "%d/%m/%Y %H:%M:%S",       # slash-separated Indian format
]


def _parse_timestamps(series: pd.Series) -> pd.Series:
    """
    Robustly parse a Series of timestamp strings into datetime objects.
    Tries multiple format strings before falling back to Pandas inference.
    """
    for fmt in _TIMESTAMP_FORMATS:
        try:
            parsed = pd.to_datetime(series, format=fmt, errors="coerce")
            # Accept this format if ≥ 95 % of non-null values parsed cleanly
            non_null = series.notna().sum()
            if non_null > 0 and (parsed.notna().sum() / non_null) >= 0.95:
                return parsed
        except Exception:
            continue

    # Final fallback: pandas mixed-format inference
    return pd.to_datetime(series, infer_datetime_format=True, errors="coerce")


def process_file(
    input_path:  Path,
    output_dir:  Path,
    risk_free:   float,
) -> dict:
    """
    Enrich a single CSV file with IV and Greeks columns.

    Returns a stats dict: {rows_total, rows_options, rows_ok, rows_fail, elapsed_s}
    """
    t0 = time.perf_counter()
    stats = {
        "rows_total":   0,
        "rows_options": 0,
        "rows_ok":      0,
        "rows_fail":    0,
        "elapsed_s":    0.0,
    }

    log.info("──────────────────────────────────────────────────────")
    log.info("Processing : %s", input_path.name)

    # ── Load ───────────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(input_path, low_memory=False, dtype=str)
    except Exception as exc:
        log.error("  SKIP — could not read file: %s", exc)
        return stats

    if df.empty:
        log.warning("  SKIP — file is empty.")
        return stats

    # Normalise column names: strip spaces, lowercase for matching
    df.columns = [c.strip() for c in df.columns]
    stats["rows_total"] = len(df)
    log.info("  Rows loaded       : %d", len(df))

    # ── Parse timestamps ──────────────────────────────────────────────────
    if COL_TIMESTAMP not in df.columns:
        log.error(
            "  SKIP — required column '%s' not found. Available: %s",
            COL_TIMESTAMP, list(df.columns),
        )
        return stats

    df[COL_TIMESTAMP] = _parse_timestamps(df[COL_TIMESTAMP])
    bad_ts = df[COL_TIMESTAMP].isna().sum()
    if bad_ts > 0:
        log.warning(
            "  %d row(s) had unparseable timestamps (will be skipped for Greeks).", bad_ts
        )

    # Convert numeric columns that may have been read as strings
    for col in (COL_LTP, COL_OI, "open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if COL_BID_ASK in df.columns:
        df[COL_BID_ASK] = pd.to_numeric(df[COL_BID_ASK], errors="coerce")

    # ── Identify option rows ───────────────────────────────────────────────
    if COL_EXCHANGE not in df.columns:
        log.error("  SKIP — required column '%s' not found.", COL_EXCHANGE)
        return stats

    is_spot = df[COL_EXCHANGE].str.strip().str.upper().isin(SPOT_EXCHANGES)
    option_mask = ~is_spot

    # Also accept NC/BC rows that parse as options (rare but possible)
    option_rows_idx = df[option_mask].index.tolist()
    stats["rows_options"] = len(option_rows_idx)
    log.info("  Option rows found : %d  (spot rows: %d)", len(option_rows_idx), is_spot.sum())

    # ── Build underlying price index from spot rows ────────────────────────
    price_index = build_underlying_price_index(df)

    # ── Pre-allocate new columns with NaN ─────────────────────────────────
    for col in GREEKS_COLS:
        df[col] = np.nan
    df["calc_status"] = "NON_OPTION"    # default for spot rows

    # ── Enrich option rows ────────────────────────────────────────────────
    ok_count   = 0
    fail_count = 0
    parse_fail = 0

    for idx in option_rows_idx:
        row      = df.loc[idx]
        enriched = enrich_row(row, price_index, risk_free)

        for col in GREEKS_COLS:
            df.at[idx, col] = enriched.get(col, np.nan)

        status = enriched.get("calc_status", "FAIL")
        if status == "OK":
            ok_count += 1
        elif status == "PARSE_FAIL":
            parse_fail += 1
            fail_count += 1
        else:
            fail_count += 1

    stats["rows_ok"]   = ok_count
    stats["rows_fail"] = fail_count

    log.info(
        "  Greeks computed   : OK=%d  FAIL=%d  PARSE_FAIL=%d",
        ok_count, fail_count, parse_fail,
    )

    # ── Write output ──────────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    stem        = input_path.stem
    output_path = output_dir / f"{stem}{OUTPUT_SUFFIX}.csv"

    try:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log.info("  Output written    : %s", output_path)
    except OSError as exc:
        log.error("  ERROR writing output: %s", exc)

    stats["elapsed_s"] = round(time.perf_counter() - t0, 2)
    log.info("  Elapsed           : %.2f s", stats["elapsed_s"])

    return stats


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def _collect_input_files(input_arg: Path) -> list[Path]:
    """
    Return a sorted list of CSV files to process.
    Accepts either a single CSV file or a directory.
    """
    if input_arg.is_file():
        if input_arg.suffix.lower() != ".csv":
            log.warning("Input file '%s' does not have a .csv extension.", input_arg)
        return [input_arg]

    if input_arg.is_dir():
        files = sorted(input_arg.glob("*.csv"))
        if not files:
            log.error("No *.csv files found in directory: %s", input_arg)
        return files

    log.error("Input path does not exist: %s", input_arg)
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-market IV and Greeks calculator for Indian F&O tick data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python options_greeks_calculator.py
  python options_greeks_calculator.py --input ..\\Tick_Data_Folder\\tick_data\\
  python options_greeks_calculator.py --input ticks_2026-03-03.csv
  python options_greeks_calculator.py --rate 0.068
        """,
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        metavar="PATH",
        help=f"Input CSV file or directory  [default: {DEFAULT_INPUT_DIR}]",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory for enriched CSVs  [default: {DEFAULT_OUTPUT_DIR}]",
    )
    parser.add_argument(
        "--rate", "-r",
        type=float,
        default=RISK_FREE_RATE,
        metavar="FLOAT",
        help=f"Annualised risk-free rate as decimal  [default: {RISK_FREE_RATE}]",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("═" * 60)
    log.info("  Options Greeks Calculator  —  post-market enrichment")
    log.info("═" * 60)
    log.info("  Input  : %s",  args.input)
    log.info("  Output : %s",  args.output)
    log.info("  r (risk-free) : %.4f  (%.2f %%)", args.rate, args.rate * 100)

    # ── Collect files ──────────────────────────────────────────────────────
    files = _collect_input_files(args.input)
    if not files:
        log.critical("No input files found. Exiting.")
        raise SystemExit(1)

    log.info("  Files to process  : %d", len(files))

    # ── Process each file ──────────────────────────────────────────────────
    total_t0       = time.perf_counter()
    grand_total    = 0
    grand_options  = 0
    grand_ok       = 0
    grand_fail     = 0

    for i, fpath in enumerate(files, start=1):
        log.info("\n[%d/%d]", i, len(files))
        s = process_file(
            input_path=fpath,
            output_dir=args.output,
            risk_free=args.rate,
        )
        grand_total   += s["rows_total"]
        grand_options += s["rows_options"]
        grand_ok      += s["rows_ok"]
        grand_fail    += s["rows_fail"]

    # ── Run summary ────────────────────────────────────────────────────────
    wall_time = round(time.perf_counter() - total_t0, 2)
    ok_rate   = (grand_ok / grand_options * 100) if grand_options else 0.0

    log.info("\n" + "═" * 60)
    log.info("  RUN SUMMARY")
    log.info("  Files processed : %d",   len(files))
    log.info("  Total rows      : %d",   grand_total)
    log.info("  Option rows     : %d",   grand_options)
    log.info("  Greeks OK       : %d  (%.1f %%)", grand_ok,   ok_rate)
    log.info("  Greeks FAIL     : %d  (%.1f %%)", grand_fail, 100 - ok_rate)
    log.info("  Total wall time : %.2f s", wall_time)
    log.info("  Output location : %s",   args.output.resolve())
    log.info("═" * 60)


if __name__ == "__main__":
    main()
