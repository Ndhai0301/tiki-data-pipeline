#!/usr/bin/env python3
"""
validate_bronze.py - Chan du lieu rac NGAY DAU luong, truoc khi no troi
xuong Spark/Postgres/dbt roi moi phat hien (ton ca pipeline chay vo ich).

Kiem tra 2 dieu kien don gian nhung du de bat cac loi da tung gap that
trong du an nay:
  1. Tong so ban ghi Bronze duoi nguong --min-rows -> nghi bi chan/captcha
     hang loat (dung xay ra o snap_20260822_00, 13/29 category bi chan).
  2. Co category nao ma so ban ghi = 0 vuot qua --max-empty-categories ->
     nghi WAF chan tung phan, khong phai loi mang thoang qua.

Exit code != 0 -> Airflow danh dau task FAIL -> DAG dung lai, khong chay
tiep spark_to_silver/load_postgres voi du lieu thieu.

Chay:
    python3 validate_bronze.py --dt 2026-08-25 --hour 00
    python3 validate_bronze.py --dt 2026-08-25 --hour 00 --min-rows 5000
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate Bronze truoc khi cho troi xuong Silver")
    p.add_argument("--dt", required=True)
    p.add_argument("--hour", default="00")
    p.add_argument("--data-dir", default="./data")
    p.add_argument("--min-rows", type=int, default=5000, help="Tong so dong toi thieu tren toan bo category")
    p.add_argument("--max-empty-categories", type=int, default=3, help="So category duoc phep = 0 dong truoc khi fail")
    args = p.parse_args(argv)

    base = Path(args.data_dir).expanduser().resolve() / "bronze" / f"dt={args.dt}" / f"hour={args.hour}"
    if not base.exists():
        print(f"LOI: khong tim thay {base}", file=sys.stderr)
        return 1

    category_dirs = sorted(base.glob("category=*"))
    if not category_dirs:
        print(f"LOI: {base} khong co category nao", file=sys.stderr)
        return 1

    total_rows = 0
    empty_categories: list[str] = []
    per_category: dict[str, int] = {}

    for cat_dir in category_dirs:
        name = cat_dir.name.removeprefix("category=")
        f = cat_dir / "listings.jsonl.gz"
        if not f.exists():
            per_category[name] = 0
            empty_categories.append(name)
            continue
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            n = sum(1 for _ in fh)
        per_category[name] = n
        total_rows += n
        if n == 0:
            empty_categories.append(name)

    print(f"Tong: {total_rows} dong tren {len(category_dirs)} category, {len(empty_categories)} category rong")

    ok = True
    if total_rows < args.min_rows:
        print(f"FAIL: tong {total_rows} dong < nguong toi thieu {args.min_rows}", file=sys.stderr)
        ok = False
    if len(empty_categories) > args.max_empty_categories:
        print(
            f"FAIL: {len(empty_categories)} category rong ({', '.join(empty_categories)}) "
            f"> nguong cho phep {args.max_empty_categories} - nghi bi WAF chan hang loat",
            file=sys.stderr,
        )
        ok = False

    if not ok:
        return 1

    print("OK: Bronze hop le, cho phep di tiep.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
