#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent))
from schema import LISTING_SCHEMA  # noqa: E402
from transform import bronze_to_silver  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Bronze -> Silver bang Spark, theo tung --dt")
    p.add_argument("--dt", required=True, help="Ngay can xu ly, VD 2026-08-22 (khop partition dt= cua Bronze)")
    p.add_argument("--data-dir", default="./data", help="Thu muc goc data/ (co bronze/)")
    args = p.parse_args(argv)

    spark = (
        SparkSession.builder
        .appName("tiki_bronze_to_silver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"{args.data_dir}/bronze/dt={args.dt}/*/*/listings.jsonl.gz"
    raw = spark.read.schema(LISTING_SCHEMA).json(input_path)

    bronze_count = raw.count()
    if bronze_count == 0:
        print(f"Khong tim thay du lieu Bronze cho dt={args.dt} o {input_path}")
        spark.stop()
        return 1

    raw = (
        raw.withColumn("_input_file", F.input_file_name())
        .withColumn("dt", F.regexp_extract("_input_file", r"dt=([^/]+)", 1))
        .withColumn("hour", F.regexp_extract("_input_file", r"hour=([^/]+)", 1))
        .withColumn("category", F.regexp_extract("_input_file", r"category=([^/]+)", 1))
        .drop("_input_file")
    )

    silver = bronze_to_silver(raw)
    silver_count = silver.count()

    out_path = f"{args.data_dir}/silver"
    (
        silver.write
        .mode("overwrite")
        .partitionBy("dt", "hour", "category")
        .parquet(out_path)
    )

    trung = bronze_count - silver_count
    print(
        f"Bronze: {bronze_count} dong | Silver: {silver_count} dong | "
        f"Trung lap da loai bo: {trung} | Output: {out_path}"
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
