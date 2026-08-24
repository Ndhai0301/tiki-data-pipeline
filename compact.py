#!/usr/bin/env python3
"""
compact.py - Gop cac file Silver Parquet nho (theo dt=/hour=/category=)
thanh 1 file lon/category/thang, giai quyet van de "file nho" khi Spark/
DuckDB phai mo hang nghin file de doc metadata.

Khong dong den Bronze (bat bien theo thiet ke - xem docs/storage.md) va
khong sua/xoa file Silver goc - chi doc va ghi ra data/silver_compacted/,
an toan de chay lai nhieu lan (idempotent, ghi de dung file thang do).

Voi volume hien tai (~1.5 GB/thang) chua thuc su can, nhung viet san de
chay dinh ky (VD dau moi thang, qua DAG/cron) truoc khi so luong file tro
thanh van de that.

Chay:
    python3 compact.py --month 2026-08
    python3 compact.py --month 2026-08 --data-dir ./data --dry-run
"""

from __future__ import annotations

import argparse
import glob
import logging
from pathlib import Path

import pandas as pd

LOG = logging.getLogger("compact")


def find_month_files(data_dir: Path, month: str) -> dict[str, list[str]]:
    """Tra ve {category: [duong dan file parquet]} cho 1 thang, gom tu moi dt/hour."""
    pattern = str(data_dir / "silver" / f"dt={month}-*" / "hour=*" / "category=*" / "products.parquet")
    files = glob.glob(pattern)

    by_category: dict[str, list[str]] = {}
    for f in files:
        for part in Path(f).parts:
            if part.startswith("category="):
                by_category.setdefault(part.removeprefix("category="), []).append(f)
                break
    return by_category


def compact_category(files: list[str], out_path: Path, dry_run: bool) -> int:
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    rows_before = len(df)

    if dry_run:
        LOG.info("[dry-run] %s: %d file -> %d dong (chua ghi)", out_path, len(files), rows_before)
        return rows_before

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression="snappy")
    LOG.info("%s: gop %d file -> %d dong -> %s", out_path.name, len(files), rows_before, out_path)
    return rows_before


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gop Silver Parquet nho theo thang")
    p.add_argument("--month", required=True, help="YYYY-MM, vd 2026-08")
    p.add_argument("--data-dir", default="./data", help="Thu muc goc data/ (co bronze/, silver/)")
    p.add_argument("--dry-run", action="store_true", help="Chi in ra se gop gi, khong ghi file")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    data_dir = Path(args.data_dir).expanduser().resolve()
    by_category = find_month_files(data_dir, args.month)

    if not by_category:
        LOG.warning(
            "Khong tim thay file nao cho thang %s trong %s "
            "(luu y: script nay doc theo cau truc dt=/hour=/category=, "
            "xem docs/storage.md muc migrate neu du lieu con nam trong snap_*/)",
            args.month, data_dir,
        )
        return 1

    total_rows = 0
    for category, files in sorted(by_category.items()):
        out_path = data_dir / "silver_compacted" / f"category={category}" / f"{args.month}.parquet"
        total_rows += compact_category(files, out_path, args.dry_run)

    LOG.info(
        "Xong. %d category, %d dong tong cong. %s",
        len(by_category), total_rows,
        "(dry-run, chua ghi file that)" if args.dry_run else f"Output: {data_dir / 'silver_compacted'}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
