#!/usr/bin/env bash
# run_pipeline.sh - Chay EL (nap Silver vao Postgres) + T (dbt run + snapshot)
# Goi sau khi tiki_crawl.py crawl xong (noi bang && trong crontab), khong
# chay doc lap theo lich rieng - tranh chay truoc/song song luc crawl chua xong.
#
# set -e: dung ngay neu buoc nao loi, khong chay tiep buoc sau voi du lieu
# co the thieu/sai.
set -euo pipefail

cd "$(dirname "$0")"
source .venv/bin/activate

echo "=========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') EL: load_silver_to_postgres.py"
echo "=========================================="
python3 load_silver_to_postgres.py --verbose

echo "=========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') T: dbt run (staging + fact_price_daily)"
echo "=========================================="
(cd dbt && dbt run --profiles-dir .)

echo "=========================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') T: dbt snapshot (dim_product SCD2)"
echo "=========================================="
(cd dbt && dbt snapshot --profiles-dir .)

echo "$(date '+%Y-%m-%d %H:%M:%S') Xong pipeline EL+T."
