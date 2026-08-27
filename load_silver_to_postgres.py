#!/usr/bin/env python3
"""
load_silver_to_postgres.py - Buoc EL (Extract-Load): doc toan bo Silver
Parquet (data/silver/dt=*/hour=*/category=*/*.parquet) va nap vao
1 bang tho trong Postgres (raw.listings) de dbt doc va transform tiep.

Day KHONG phai dbt - dbt khong doc duoc file Parquet tren dia, chi chay
SQL "trong" mot database. Script nay lam phan EL, dbt lam phan T (xem
docs/storage.md va cuoc trao doi ve kien truc EL/T).

Dung DuckDB (co san, khong can driver rieng) lam cau noi: doc Parquet bang
glob + Hive partitioning, ghi thang vao Postgres qua extension postgres
cua DuckDB - khong can vong lap Python/pandas.to_sql() cham.

Chien luoc: FULL REFRESH - moi lan chay xoa va nap lai toan bo bang raw.
Don gian, luon dung, phu hop voi volume hien tai (~60K dong). Neu volume
tang qua lon (hang chuc trieu dong) thi doi sang nap incremental (chi nap
dt/hour moi), nhung chua can o quy mo nay.

Cai dat:
    pip install duckdb

Chay:
    python3 load_silver_to_postgres.py
    python3 load_silver_to_postgres.py --data-dir ./data --pg-dsn "host=localhost port=5433 dbname=tiki user=tiki password=tiki"
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import duckdb

LOG = logging.getLogger("load")

# Chay tu host (ngoai docker): postgres lo ra port 5433. Chay tu trong
# container Airflow (cung docker network voi service "postgres"): phai
# noi qua ten service + port noi bo 5432, "localhost" trong container la
# chinh container do, khong phai host hay container postgres. Dat bien moi
# truong PG_DSN trong docker-compose.yml cho container Airflow de ghi de.
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
    # *.parquet (khong phai "products.parquet" co dinh): pandas ghi 1 file ten
    # co dinh, Spark ghi nhieu file "part-*.parquet" - glob nay doc duoc ca 2.
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
    # CASCADE: cac view/model dbt (vd staging.stg_listings) phu thuoc bang
    # nay se bi xoa theo, dbt se tu tao lai khi chay "dbt run" sau buoc nay.
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
