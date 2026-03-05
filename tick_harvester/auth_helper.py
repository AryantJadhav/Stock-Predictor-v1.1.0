#!/usr/bin/env python3
"""
auth_helper.py
==============
One-time interactive helper to obtain a fresh Sharekhan access_token
and save it to config.json.

Run this script:
  • Once manually on your local machine (or EC2 via SSH) before the first harvest.
  • Every morning via cron before market open, since the access_token typically
    expires at the end of each trading day (00:00 IST or server-side reset).

Usage:
    python auth_helper.py

Cron example (refresh every day at 08:45 IST = 03:15 UTC):
    15 3 * * 1-5  /home/ubuntu/tick_harvester/venv/bin/python \
                  /home/ubuntu/tick_harvester/auth_helper.py \
                  --request-token "$SK_REQUEST_TOKEN"

    (where $SK_REQUEST_TOKEN is obtained via a headless Selenium flow or
     manually pasted from the Sharekhan redirect URL each morning)
"""

import argparse
import json
import sys
import webbrowser
from pathlib import Path

# ---------------------------------------------------------------------------
try:
    from SharekhanApi.sharekhanConnect import SharekhanConnect  # type: ignore
except ImportError:
    print(
        "ERROR: 'shareconnect' package not found.\n"
        "       Run:  pip install shareconnect"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
CONFIG_FILE = Path(__file__).parent / "config.json"

# A fixed arbitrary integer required by the SDK's get_access_token()
# (it is echoed back in the OAuth response for CSRF validation)
_OAUTH_STATE = 12345


# ═══════════════════════════════════════════════════════════════════
def _load_config() -> dict:
    """Read existing config.json or return an empty dict."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_config(cfg: dict) -> None:
    """Write config dict to config.json with 4-space indentation."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=4)
    print(f"\n  ✔  config.json saved → {CONFIG_FILE}")


def _prompt(label: str, current: str) -> str:
    """Interactive prompt that keeps the current value if the user presses Enter."""
    display = f"[{current}]" if current else ""
    value = input(f"  {label:<18}{display}: ").strip()
    return value or current


# ═══════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Obtain a Sharekhan access_token and save it to config.json"
    )
    parser.add_argument(
        "--request-token",
        metavar="TOKEN",
        default=None,
        help=(
            "Pass the raw request_token directly (useful for cron/automation). "
            "If omitted, the script opens the browser and prompts interactively."
        ),
    )
    args = parser.parse_args()

    # ── Load existing values ────────────────────────────────────────────────
    cfg = _load_config()

    print()
    print("═" * 60)
    print("   Sharekhan Auth Helper")
    print("═" * 60)

    # ── Collect credentials ─────────────────────────────────────────────────
    cfg["api_key"]     = _prompt("API Key",      cfg.get("api_key",      ""))
    cfg["secret_key"]  = _prompt("Secret Key",   cfg.get("secret_key",   ""))
    cfg["customer_id"] = _prompt("Customer ID",  cfg.get("customer_id",  ""))

    if not cfg["api_key"] or not cfg["secret_key"]:
        print("\nERROR: api_key and secret_key are required.")
        sys.exit(1)

    # ── Step 1: Generate the browser login URL ──────────────────────────────
    print("\n" + "─" * 60)
    print("  Step 1 — Generating login URL …")
    print("─" * 60)

    login = SharekhanConnect(cfg["api_key"])

    # vendor_key="" for direct (non-vendor) login
    # version_id="" means use the default (no version-specific flow)
    login_url = login.login_url(vendor_key="", version_id="")

    print(f"\n  Open this URL in your browser and log in with your Sharekhan credentials:\n")
    print(f"    {login_url}\n")
    print(
        "  After a successful login Sharekhan will redirect you to a URL that "
        "contains\n  the 'RequestToken' query parameter, e.g.:\n"
        "  https://yourredirect.example/?RequestToken=xxxxx&CustomerId=yyyyy\n"
    )

    # Try to open the system browser (works on a desktop; silently fails on EC2)
    try:
        webbrowser.open(login_url)
    except Exception:
        pass  # headless EC2 — user copies the URL manually

    # ── Step 2: Collect the request_token ──────────────────────────────────
    if args.request_token:
        request_token = args.request_token.strip()
        print(f"  Using --request-token: {request_token[:12]}…")
    else:
        print("─" * 60)
        print("  Step 2 — Paste the request_token from the redirect URL")
        print("─" * 60)
        request_token = input("  request_token: ").strip()

    if not request_token:
        print("\nERROR: request_token cannot be empty.")
        sys.exit(1)

    # ── Step 3: Generate session & access_token ─────────────────────────────
    print("\n─" * 60)
    print("  Step 3 — Generating session …")
    print("─" * 60)

    try:
        # Use the version-less method (most common for retail API access).
        # Switch to login.generate_session(request_token, secret_key)
        # if you were issued a version_id by Sharekhan.
        session = login.generate_session_without_versionId(
            request_token,
            cfg["secret_key"],
        )
        access_token = login.get_access_token(cfg["api_key"], session, _OAUTH_STATE)

    except Exception as exc:
        print(f"\nERROR: Could not obtain access_token — {exc}")
        print("  Verify your api_key, secret_key, and that the request_token is fresh.")
        sys.exit(1)

    if not access_token:
        print(
            "\nERROR: get_access_token() returned an empty value.\n"
            "  The request_token may have expired (they are single-use and short-lived).\n"
            "  Please try again from Step 1."
        )
        sys.exit(1)

    # ── Step 4: Save to config.json ─────────────────────────────────────────
    cfg["access_token"] = access_token
    _save_config(cfg)

    # Show a truncated token so the user can verify it looks right
    short = access_token[:16] + "…" if len(access_token) > 16 else access_token
    print(f"\n  access_token = {short}")
    print("\n  You can now start the harvester:")
    print("    python tick_harvester.py\n")


if __name__ == "__main__":
    main()
