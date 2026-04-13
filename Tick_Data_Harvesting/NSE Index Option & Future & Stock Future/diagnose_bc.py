#!/usr/bin/env python3
"""
diagnose_bc.py
==============
Dumps the first 5 records from the BC (BSE Cash) master so we can
see the exact field names and symbol values that Sharekhan returns.
Then searches all 15k+ records for SENSEX / BANKEX variants.
Run once, share the output with the developer.
"""
import json
import os
from pathlib import Path
from SharekhanApi.sharekhanConnect import SharekhanConnect  # type: ignore

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"

with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
    cfg = json.load(fh)

access_raw = cfg.get("access_token", "")
if isinstance(access_raw, dict):
    access_token = access_raw.get("token", "")
else:
    access_token = str(access_raw).strip()

api_key = cfg.get("api_key", "")
sk = SharekhanConnect(api_key, access_token)

print("Fetching BC master...")
raw = sk.master("BC")

# Normalise to list
if isinstance(raw, str):
    records = json.loads(raw)
elif isinstance(raw, list):
    records = raw
elif isinstance(raw, dict):
    for k in ("data", "result", "records", "scriptMaster", "ScriptMaster"):
        if isinstance(raw.get(k), list):
            records = raw[k]
            break
    else:
        records = []
else:
    records = []

print(f"\nTotal BC records: {len(records)}")

# --- Show first 3 records in full ---
print("\n=== FIRST 3 RECORDS (full) ===")
for i, r in enumerate(records[:3]):
    print(f"\n[{i}] {json.dumps(r, indent=2)}")

# --- Search for SENSEX / BANKEX / INDEX variants ---
print("\n=== SEARCHING FOR SENSEX / BANKEX / INDEX ===")
KEYWORDS = ["sensex", "bankex", "bsesn", "index", "bse 30", "bse30", "s&p bse"]
hits = []
for r in records:
    row_str = json.dumps(r).lower()
    if any(kw in row_str for kw in KEYWORDS):
        hits.append(r)

print(f"Found {len(hits)} matching records.")
for r in hits[:20]:
    print(json.dumps(r))

# --- Dump all unique field names seen ---
print("\n=== ALL FIELD NAMES IN BC MASTER ===")
all_keys = set()
for r in records[:500]:
    if isinstance(r, dict):
        all_keys.update(r.keys())
print(sorted(all_keys))
