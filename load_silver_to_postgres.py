#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import duckdb

LOG = logging.getLogger("load")

DEFAULT_PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5433 dbname=tiki user=tiki password=tiki"
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Nap Silver Parquet vao Postgres (bang raw.listings)")
    p.add_argument("--data-dir", default="./data", help="Thu muc goc data/ (co silver/)")
    p.add_argument("--pg-dsn", default=DEFAULT_PG_DSN, help="Chuoi ket noi Postgres (libpq format)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    data_dir = Path(args.data_dir).expanduser().resolve()
    parquet_glob = str(data_dir / "silver" / "*" / "*" / "*" / "*.parquet")

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{args.pg_dsn}' AS pg (TYPE POSTGRES);")

    LOG.info("Doc Parquet tu: %s", parquet_glob)
    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}', hive_partitioning=true)"
    ).fetchone()[0]

    if row_count == 0:
        LOG.warning("Khong tim thay dong nao o %s - kiem tra lai --data-dir", parquet_glob)
        return 1

    LOG.info("Tim thay %d dong trong Silver. Nap vao pg.raw.listings (full refresh)...", row_count)

    con.execute("CREATE SCHEMA IF NOT EXISTS pg.raw;")
    con.execute("DROP TABLE IF EXISTS pg.raw.listings CASCADE;")
    con.execute(f"""
        CREATE TABLE pg.raw.listings AS
        SELECT *
        FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)

    loaded = con.execute("SELECT COUNT(*) FROM pg.raw.listings").fetchone()[0]
    LOG.info("Xong. pg.raw.listings: %d dong.", loaded)

    con.close()
    return 0 if loaded == row_count else 1


if __name__ == "__main__":
    raise SystemExit(main())
