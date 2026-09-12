#!/usr/bin/env python3
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
