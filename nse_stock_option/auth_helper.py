#!/usr/bin/env python3
"""
auth_helper.py
==============
Daily token refresh helper for Sharekhan (multi-client aware).

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
    python nse_stock_options_auth_helper.py --client api1
    python nse_stock_options_auth_helper.py --client api2

Automation (cron / Task Scheduler) — pass the token directly:
    python nse_stock_options_auth_helper.py --request-token <REQUEST_TOKEN>

Cron example (every weekday at 09:00 IST = 03:30 UTC):
    30 3 * * 1-5  cd /home/ubuntu/tick_harvester && \
                  venv/bin/python nse_stock_options_auth_helper.py --client api1 --request-token "$SK_REQUEST_TOKEN"
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
PARENT_CONFIG_FILE = Path(__file__).parent.parent / "tick_data" / "config.json"
SECTOR_SYMBOLS_FILE = Path(__file__).parent / "sector_symbols.json"

DEFAULT_CLIENT_NAME = "api1"

# Legacy fallback defaults when api_clients is not configured yet.
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

    customer_id and smtp fields are copied from parent folder
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
        "_comment": "Dedicated config for NSE Stock Options worker.",
        "api_clients": [
            {
                "name": "api1",
                "api_key": DEFAULT_API_KEY,
                "secret_key": DEFAULT_SECRET_KEY,
                "access_token": {
                    "token": "",
                    "updated_on": "",
                    "updated_at": "",
                },
                "symbols_file": "fo_symbols_api1.txt",
                "output_subdir": "api1",
            }
        ],
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


def _get_client_index(cfg: dict, client_name: str) -> int | None:
    raw_clients = cfg.get("api_clients")
    if not isinstance(raw_clients, list):
        return None

    for idx, raw_client in enumerate(raw_clients):
        if not isinstance(raw_client, dict):
            continue
        name = str(raw_client.get("name", "")).strip()
        if name == client_name:
            return idx
    return None


def _get_client_credentials(cfg: dict, client_name: str) -> tuple[str, str, int | None]:
    idx = _get_client_index(cfg, client_name)
    if idx is not None:
        client = cfg["api_clients"][idx]
        api_key = str(client.get("api_key", "")).strip()
        secret_key = str(client.get("secret_key", "")).strip()
        return api_key, secret_key, idx

    # Legacy single-client fallback
    api_key = str(cfg.get("api_key", "")).strip()
    secret_key = str(cfg.get("secret_key", "")).strip()
    return api_key, secret_key, None


def _validate_config(cfg: dict, client_name: str) -> None:
    missing = [k for k in ("customer_id",) if not cfg.get(k)]

    api_key, secret_key, _ = _get_client_credentials(cfg, client_name)
    if not api_key:
        missing.append("api_key")
    if not secret_key:
        missing.append("secret_key")

    if missing:
        sys.exit(
            f"ERROR: The following fields are missing or empty in config.json:\n"
            + "".join(f"  • {k}\n" for k in missing)
        )


def _apply_worker_credentials(cfg: dict, client_name: str) -> tuple[str, str, int | None]:
    """Resolve credentials for selected client. Env vars override file values."""
    file_api_key, file_secret_key, client_idx = _get_client_credentials(cfg, client_name)

    api_key = os.getenv("SHAREKHAN_API_KEY", file_api_key or DEFAULT_API_KEY).strip()
    secret_key = os.getenv("SHAREKHAN_SECRET_KEY", file_secret_key or DEFAULT_SECRET_KEY).strip()

    return api_key, secret_key, client_idx


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
        "--client",
        metavar="NAME",
        default=DEFAULT_CLIENT_NAME,
        help="Client name from config.json api_clients (default: api1).",
    )
    parser.add_argument(
        "--request-token",
        metavar="TOKEN",
        default=None,
        help="Supply the request_token directly (for cron/automation).",
    )
    args = parser.parse_args()
    client_name = str(args.client).strip() or DEFAULT_CLIENT_NAME

    # ── Load & validate static credentials ───────────────────────────────────
    cfg = _load_config()
    api_key, secret_key, client_idx = _apply_worker_credentials(cfg, client_name)
    _validate_config(cfg, client_name)

    print()
    print(HEADER)
    print("   Sharekhan Daily Token Refresh")
    print(HEADER)
    print(f"  Client Name : {client_name}")
    print(f"  Customer ID : {cfg['customer_id']}")
    print(f"  API Key     : {api_key[:8]}{'*' * (len(api_key) - 8)}")

    # ── Step 1: Build & open login URL ───────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  STEP 1 — Open the Sharekhan login page")
    print(DIVIDER)

    sk = SharekhanConnect(api_key)
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
            secret_key,
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
        access_token = sk.get_access_token(api_key, session, _OAUTH_STATE)
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

    # Keep top-level values in sync for backward compatibility.
    cfg["api_key"] = api_key
    cfg["secret_key"] = secret_key
    cfg["access_token"] = {
        "token": access_token,
        "updated_on": now.date().isoformat(),
        "updated_at": now.isoformat(timespec="seconds"),
    }

    # If multi-client config exists, persist token under the selected client.
    if client_idx is not None:
        client = cfg["api_clients"][client_idx]
        client["api_key"] = api_key
        client["secret_key"] = secret_key
        client["access_token"] = {
            "token": access_token,
            "updated_on": now.date().isoformat(),
            "updated_at": now.isoformat(timespec="seconds"),
        }

    _save_config(cfg)

    short = access_token[:16] + "…" if len(access_token) > 16 else access_token
    print(f"\n  ✔  access_token saved  ({short})")
    _summarize_sector_symbols()
    print("\n  Start the harvester:")
    print("    python nse_stock_options_harvester.py\n")


if __name__ == "__main__":
    main()
