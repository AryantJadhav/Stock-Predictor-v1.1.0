#!/usr/bin/env python3
"""
auth_helper.py
==============
Daily token refresh helper for Sharekhan.

The Sharekhan access_token expires every day at midnight IST.
Run this script each morning before starting the harvester.

HOW IT WORKS
------------
1. Your static credentials (api_key, secret_key, customer_id) are read
   silently from config.json — you never need to re-enter them.
2. The script prints a login URL and opens it in your browser.
3. You log in on the Sharekhan website; it redirects you to a URL like:
       https://yourredirect/?RequestToken=XXXX&CustomerId=YYYY
4. Copy the RequestToken value and paste it here when prompted.
5. The script exchanges it for a fresh access_token and saves it to
   config.json automatically.

Usage:
    python auth_helper.py

Automation (cron / Task Scheduler) — pass the token directly:
    python auth_helper.py --request-token <REQUEST_TOKEN>

Cron example (every weekday at 09:00 IST = 03:30 UTC):
    30 3 * * 1-5  cd /home/ubuntu/tick_harvester && \
                  venv/bin/python auth_helper.py --request-token "$SK_REQUEST_TOKEN"
"""

import argparse
import json
import os
import re
import sys
import webbrowser
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

try:
    from SharekhanApi.sharekhanConnect import SharekhanConnect  # type: ignore
except ImportError:
    sys.exit(
        "ERROR: 'SharekhanApi' package not found.\n"
        "       Run:  pip install -r requirements.txt"
    )

CONFIG_FILE = Path(__file__).parent / "config.json"
PARENT_CONFIG_FILE = Path(__file__).parent.parent / "config.json"
SECTOR_SYMBOLS_FILE = Path(__file__).parent / "sector_symbols.json"

# Dedicated credentials for this BSE worker.
# Environment variables (SHAREKHAN_API_KEY / SHAREKHAN_SECRET_KEY) can still
# override these defaults when needed.
DEFAULT_API_KEY = "UEje0vilIR7bP07UJcoqmad5yaGy61RP"
DEFAULT_SECRET_KEY = "8yYfGxhuEsF2aQGOfz6KoF584StLXj1J"

# Arbitrary integer echoed back by the OAuth server for CSRF validation.
_OAUTH_STATE = 12345

DIVIDER = "─" * 60
HEADER  = "═" * 60


# ── helpers ──────────────────────────────────────────────────────────────────

def _build_default_config() -> dict:
    """
    Build initial worker config.json content.

    customer_id and smtp fields are copied from the parent Tick_Data_Folder
    config when available so migration is seamless.
    """
    parent_cfg: dict = {}
    if PARENT_CONFIG_FILE.exists():
        try:
            with open(PARENT_CONFIG_FILE, "r", encoding="utf-8") as fh:
                maybe_parent = json.load(fh)
                if isinstance(maybe_parent, dict):
                    parent_cfg = maybe_parent
        except (OSError, json.JSONDecodeError):
            parent_cfg = {}

    customer_id = os.getenv("SHAREKHAN_CUSTOMER_ID", str(parent_cfg.get("customer_id", ""))).strip()

    return {
        "_comment": "Dedicated config for BSE Stock Data Harveseter worker.",
        "api_key": DEFAULT_API_KEY,
        "secret_key": DEFAULT_SECRET_KEY,
        "customer_id": customer_id,
        "smtp_sender": str(parent_cfg.get("smtp_sender", "")).strip(),
        "smtp_receiver": str(parent_cfg.get("smtp_receiver", "")).strip(),
        "smtp_password": str(parent_cfg.get("smtp_password", "")).strip(),
        "smtp_host": str(parent_cfg.get("smtp_host", "smtp.gmail.com")).strip(),
        "smtp_port": int(parent_cfg.get("smtp_port", 587)),
        "access_token": {
            "token": "",
            "updated_on": "",
            "updated_at": "",
        },
    }

def _load_config() -> dict:
    if not CONFIG_FILE.exists():
        cfg = _build_default_config()
        with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, indent=4)
        print(f"INFO: Created worker config at {CONFIG_FILE}")
        return cfg
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        sys.exit(f"ERROR: config.json is not valid JSON — {exc}")


def _save_config(cfg: dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=4)


def _validate_config(cfg: dict) -> None:
    missing = [k for k in ("api_key", "secret_key", "customer_id") if not cfg.get(k)]
    if missing:
        sys.exit(
            f"ERROR: The following fields are missing or empty in config.json:\n"
            + "".join(f"  • {k}\n" for k in missing)
        )


def _apply_worker_credentials(cfg: dict) -> None:
    """Inject dedicated BSE worker credentials (env vars take precedence)."""
    cfg["api_key"] = os.getenv("SHAREKHAN_API_KEY", DEFAULT_API_KEY).strip()
    cfg["secret_key"] = os.getenv("SHAREKHAN_SECRET_KEY", DEFAULT_SECRET_KEY).strip()


def _summarize_sector_symbols() -> None:
    """Validate optional sector_symbols.json and print a short summary."""
    if not SECTOR_SYMBOLS_FILE.exists():
        print("  Note: sector_symbols.json not found (harvester will use fo_symbols.txt only).")
        return

    try:
        with open(SECTOR_SYMBOLS_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except json.JSONDecodeError as exc:
        print(f"  Warning: sector_symbols.json is invalid JSON — {exc}")
        return
    except OSError as exc:
        print(f"  Warning: Could not read sector_symbols.json — {exc}")
        return

    if not isinstance(payload, dict):
        print("  Warning: sector_symbols.json must be a JSON object: {\"SECTOR\": [\"SYMBOL\"]}")
        return

    symbols: set[str] = set()
    sectors = 0
    for sector, items in payload.items():
        if not isinstance(items, list):
            print(f"  Warning: sector '{sector}' is not an array; skipped.")
            continue
        sectors += 1
        for raw_symbol in items:
            symbol = str(raw_symbol).strip().upper()
            if symbol:
                symbols.add(symbol)

    print(f"  Sector symbol file ready: sectors={sectors}, symbols={len(symbols)}")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh the daily Sharekhan access_token and save to config.json"
    )
    parser.add_argument(
        "--request-token",
        metavar="TOKEN",
        default=None,
        help="Supply the request_token directly (for cron/automation).",
    )
    args = parser.parse_args()

    # ── Load & validate static credentials ───────────────────────────────────
    cfg = _load_config()
    _apply_worker_credentials(cfg)
    _validate_config(cfg)

    print()
    print(HEADER)
    print("   Sharekhan Daily Token Refresh")
    print(HEADER)
    print(f"  Customer ID : {cfg['customer_id']}")
    print(f"  API Key     : {cfg['api_key'][:8]}{'*' * (len(cfg['api_key']) - 8)}")

    # ── Step 1: Build & open login URL ───────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  STEP 1 — Open the Sharekhan login page")
    print(DIVIDER)

    sk = SharekhanConnect(cfg["api_key"])
    login_url = sk.login_url(vendor_key="", version_id="")

    print(f"\n  {login_url}\n")
    print(
        "  Log in with your Sharekhan credentials. After a successful login you\n"
        "  will be redirected to a URL like:\n"
        "    http://127.0.0.1/?RequestToken=XXXXXXXX&state=12345\n"
        "  You can paste either the FULL URL or just the token value.\n"
    )

    try:
        webbrowser.open(login_url)
    except Exception:
        pass  # silently skip on headless servers

    # ── Step 2: Accept the request_token ─────────────────────────────────────
    if args.request_token:
        request_token = args.request_token.strip()
        print(f"  Using --request-token: {request_token[:12]}…")
    else:
        print(DIVIDER)
        print("  STEP 2 — Paste the RequestToken from the redirect URL")
        print(DIVIDER)
        try:
            request_token = input("\n  RequestToken: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)

    if not request_token:
        sys.exit("ERROR: RequestToken cannot be empty.")

    # Handle full URL paste — extract token using regex to avoid parse_qs
    # converting '+' → ' ' which corrupts the base64-encoded token.
    if request_token.startswith("http"):
        match = re.search(r'[?&][Rr]equest[_]?[Tt]oken=([^&]+)', request_token)
        if not match:
            sys.exit("ERROR: Could not find RequestToken in the pasted URL.")
        # unquote() decodes %2B→+ and %3D→= but leaves raw '+' untouched
        request_token = unquote(match.group(1))
        print("  Extracted RequestToken from URL.")
    else:
        # Only decode %xx sequences; do NOT call unquote_plus — it turns + into space
        request_token = unquote(request_token)

    print(f"  Token length  : {len(request_token)} chars")

    # ── Step 3: Exchange for access_token ─────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  STEP 3 — Generating access_token …")
    print(DIVIDER)

    try:
        session = sk.generate_session_without_versionId(
            request_token,
            cfg["secret_key"],
        )
    except Exception as exc:
        sys.exit(
            f"\nERROR: generate_session failed — {exc}\n"
            "  Check that your secret_key is correct and the RequestToken is fresh."
        )

    # Validate session — SDK may return an error dict instead of raising
    if not session or (isinstance(session, dict) and session.get("status") == "fail"):
        sys.exit(
            f"\nERROR: Session generation failed.\n"
            f"  Response: {session}\n"
            "  The RequestToken may have already been used or expired. Restart from Step 1."
        )

    try:
        access_token = sk.get_access_token(cfg["api_key"], session, _OAUTH_STATE)
    except Exception as exc:
        sys.exit(f"\nERROR: get_access_token failed — {exc}")

    # SDK may return the full response dict instead of just the token string.
    # Extract the token from data.token if needed.
    if isinstance(access_token, dict):
        access_token = (
            access_token.get("data", {}).get("token")
            or access_token.get("token")
            or access_token.get("access_token")
        )

    # Validate — must be a non-empty string
    if not access_token or not isinstance(access_token, str) or len(access_token) < 10:
        sys.exit(
            f"\nERROR: Received invalid access_token: {access_token!r}\n"
            "  The RequestToken may have already been used or expired.\n"
            "  Please restart from Step 1."
        )

    # ── Step 4: Persist to config.json ───────────────────────────────────────
    now = datetime.now()
    cfg["api_key"] = cfg["api_key"]
    cfg["secret_key"] = cfg["secret_key"]
    cfg["access_token"] = {
        "token": access_token,
        "updated_on": now.date().isoformat(),
        "updated_at": now.isoformat(timespec="seconds"),
    }
    _save_config(cfg)

    short = access_token[:16] + "…" if len(access_token) > 16 else access_token
    print(f"\n  ✔  access_token saved  ({short})")
    _summarize_sector_symbols()
    print("\n  Start the harvester:")
    print("    python bse_stock_harvester.py\n")


if __name__ == "__main__":
    main()
