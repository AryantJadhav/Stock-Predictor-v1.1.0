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
import sys
import webbrowser
from pathlib import Path

try:
    from SharekhanApi.sharekhanConnect import SharekhanConnect  # type: ignore
except ImportError:
    sys.exit(
        "ERROR: 'SharekhanApi' package not found.\n"
        "       Run:  pip install -r requirements.txt"
    )

CONFIG_FILE = Path(__file__).parent / "config.json"

# Arbitrary integer echoed back by the OAuth server for CSRF validation.
_OAUTH_STATE = 12345

DIVIDER = "─" * 60
HEADER  = "═" * 60


# ── helpers ──────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    if not CONFIG_FILE.exists():
        sys.exit(f"ERROR: config.json not found at {CONFIG_FILE}")
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
        "  will be redirected to a URL containing:  ?RequestToken=XXXXX\n"
        "  Copy that RequestToken value.\n"
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

    # ── Step 3: Exchange for access_token ─────────────────────────────────────
    print(f"\n{DIVIDER}")
    print("  STEP 3 — Generating access_token …")
    print(DIVIDER)

    try:
        session = sk.generate_session_without_versionId(
            request_token,
            cfg["secret_key"],
        )
        access_token = sk.get_access_token(cfg["api_key"], session, _OAUTH_STATE)
    except Exception as exc:
        sys.exit(
            f"\nERROR: Could not obtain access_token — {exc}\n"
            "  Check that your api_key / secret_key are correct and that the\n"
            "  RequestToken is fresh (they are single-use and expire quickly)."
        )

    if not access_token:
        sys.exit(
            "\nERROR: Received an empty access_token.\n"
            "  The RequestToken may have already been used or expired.\n"
            "  Please restart from Step 1."
        )

    # ── Step 4: Persist to config.json ───────────────────────────────────────
    cfg["access_token"] = access_token
    _save_config(cfg)

    short = access_token[:16] + "…" if len(access_token) > 16 else access_token
    print(f"\n  ✔  access_token saved  ({short})")
    print("\n  Start the harvester:")
    print("    python tick_harvester.py\n")


if __name__ == "__main__":
    main()
