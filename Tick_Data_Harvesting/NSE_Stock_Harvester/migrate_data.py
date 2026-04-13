#!/usr/bin/env python3
"""
One-time migration script for tick CSVs.

Old layout:
  tick_data/fo_data/DD-MM-YYYY/*.csv
  tick_data/stock_price_data$/DD-MM-YYYY/*.csv

New layout:
  tick_data/nse_fo_data/DD-MM-YYYY/*.csv
  tick_data/bse_fo_data/DD-MM-YYYY/*.csv
  tick_data/nse_stock_data/DD-MM-YYYY/*.csv
  tick_data/bse_stock_data/DD-MM-YYYY/*.csv

Design constraints:
  - Zero pandas (csv module only)
  - Row-by-row processing (RAM-safe for 2 GB EC2)
  - Exchange-based routing from CSV "Exchange" column
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

DATE_DIR_RE = re.compile(r"^\d{2}-\d{2}-\d{4}$")

EXCHANGE_TO_BUCKET: dict[str, str] = {
    "NC": "nse_stock_data",
    "BC": "bse_stock_data",
    "NF": "nse_fo_data",
    "BF": "bse_fo_data",
    "BFO": "bse_fo_data",
}


def _resolve_stock_source_dir(tick_data_root: Path) -> Path | None:
    """Prefer legacy stock_price_data$, fall back to stock_price_data."""
    with_dollar = tick_data_root / "stock_price_data$"
    if with_dollar.exists():
        return with_dollar

    without_dollar = tick_data_root / "stock_price_data"
    if without_dollar.exists():
        return without_dollar

    return None


def _iter_date_dirs(base_dir: Path):
    if not base_dir.exists():
        return

    for child in sorted(base_dir.iterdir()):
        if child.is_dir() and DATE_DIR_RE.match(child.name):
            yield child


def _open_csv_reader(csv_path: Path):
    """Try UTF-8 first, then UTF-8-SIG for BOM-affected files."""
    for enc in ("utf-8", "utf-8-sig"):
        try:
            fh = open(csv_path, "r", newline="", encoding=enc)
            reader = csv.DictReader(fh)
            return fh, reader
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Unable to decode {csv_path}")


def _route_dest_file(tick_data_root: Path, date_dir: str, src_name: str, exchange: str) -> Path | None:
    bucket = EXCHANGE_TO_BUCKET.get(exchange.upper().strip())
    if not bucket:
        return None

    target_dir = tick_data_root / bucket / date_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / src_name


def migrate_one_file(csv_path: Path, tick_data_root: Path, date_dir: str) -> tuple[int, int, int]:
    """
    Returns: (total_rows, migrated_rows, skipped_rows)
    """
    total_rows = 0
    migrated_rows = 0
    skipped_rows = 0

    handle, reader = _open_csv_reader(csv_path)
    try:
        fieldnames = reader.fieldnames
        if not fieldnames:
            return total_rows, migrated_rows, skipped_rows

        output_handles: dict[Path, tuple[object, csv.DictWriter]] = {}

        try:
            for row in reader:
                total_rows += 1
                exchange = str(row.get("Exchange", "")).strip().upper()
                target_file = _route_dest_file(tick_data_root, date_dir, csv_path.name, exchange)
                if target_file is None:
                    skipped_rows += 1
                    continue

                if target_file not in output_handles:
                    file_exists = target_file.exists() and target_file.stat().st_size > 0
                    out_fh = open(target_file, "a", newline="", encoding="utf-8")
                    writer = csv.DictWriter(out_fh, fieldnames=fieldnames, extrasaction="ignore")
                    if not file_exists:
                        writer.writeheader()
                    output_handles[target_file] = (out_fh, writer)

                output_handles[target_file][1].writerow(row)
                migrated_rows += 1
        finally:
            for out_fh, _writer in output_handles.values():
                out_fh.close()
    finally:
        handle.close()

    return total_rows, migrated_rows, skipped_rows


def migrate_tree(source_base: Path, tick_data_root: Path, label: str) -> tuple[int, int, int, int]:
    """
    Returns: (files, total_rows, migrated_rows, skipped_rows)
    """
    if not source_base.exists():
        print(f"[SKIP] {label}: source path missing -> {source_base}")
        return (0, 0, 0, 0)

    files = 0
    total_rows = 0
    migrated_rows = 0
    skipped_rows = 0

    for date_path in _iter_date_dirs(source_base):
        print(f"[DATE] Migrating {label} :: {date_path.name}")

        for csv_path in sorted(date_path.glob("*.csv")):
            files += 1
            t, m, s = migrate_one_file(csv_path, tick_data_root, date_path.name)
            total_rows += t
            migrated_rows += m
            skipped_rows += s
            print(
                f"  [FILE] {csv_path.name}  rows={t} migrated={m} skipped={s}"
            )

    return (files, total_rows, migrated_rows, skipped_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate old tick_data layout to NSE/BSE split folders.")
    parser.add_argument(
        "--tick-data-root",
        default=str(Path(__file__).parent / "tick_data"),
        help="Path to tick_data root (default: Tick_Data_Folder/tick_data)",
    )
    args = parser.parse_args()

    tick_data_root = Path(args.tick_data_root).resolve()
    old_fo = tick_data_root / "fo_data"
    old_stock = _resolve_stock_source_dir(tick_data_root)

    print("[START] Migration started")
    print(f"  tick_data_root: {tick_data_root}")
    print(f"  old_fo_source: {old_fo}")
    print(f"  old_stock_source: {old_stock if old_stock else 'NOT FOUND'}")

    fo_stats = migrate_tree(old_fo, tick_data_root, "fo_data")
    stock_stats = (0, 0, 0, 0)
    if old_stock:
        stock_stats = migrate_tree(old_stock, tick_data_root, old_stock.name)

    total_files = fo_stats[0] + stock_stats[0]
    total_rows = fo_stats[1] + stock_stats[1]
    total_migrated = fo_stats[2] + stock_stats[2]
    total_skipped = fo_stats[3] + stock_stats[3]

    print("[SUMMARY]")
    print(f"  files_processed={total_files}")
    print(f"  rows_total={total_rows}")
    print(f"  rows_migrated={total_migrated}")
    print(f"  rows_skipped={total_skipped}")
    print("[DONE] Migration complete")


if __name__ == "__main__":
    main()
