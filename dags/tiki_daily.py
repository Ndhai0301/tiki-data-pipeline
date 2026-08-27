"""
tiki_daily - Pipeline chinh thuc, thay hoan toan cron truoc day.

crawl_tiki -> validate_bronze -> spark_to_silver -> load_postgres
    -> dbt_snapshot -> dbt_run -> dbt_test -> refresh_dashboard

3 nguyen tac bat buoc (xem docs/storage.md va cuoc trao doi ve idempotent):

1. Idempotent theo {{ ds }}: crawl_tiki nhan --dt {{ ds }}, ghi vao dung
   partition dt=/hour=00 - chay lai DAG cho cung ngay se GHI DE dung
   partition do (khong nhan doi), KHONG phai chay lai se ra gia tri
   Y HET nhu lan truoc (Tiki tra ve gia HIEN TAI, co the da doi giua 2
   lan retry - day la gioi han that cua nguon du lieu, khong phai bug).

2. catchup=False: khong the crawl lai gia CUA QUA KHU (Tiki chi co API
   tra gia hien tai). Neu DAG bi tat vai ngay roi bat lai, KHONG duoc tu
   dong chay bu cac ngay da bo lo - se tao du lieu SAI (gia "hom qua"
   thuc chat la gia luc chay bu, gan nham cho ngay cu).

3. Fail nhanh dung cho: validate_bronze chan ngay sau crawl_tiki, truoc
   khi Spark/Postgres/dbt kip chay voi du lieu rac.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO = "/opt/tiki-crawl"
DATA_DIR = f"{REPO}/data"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


def alert_callback(context):
    """SLA/loi callback - hien chi LOG, chua noi Discord/email that (xem
    ghi chu trong cuoc trao doi luc tao DAG nay - de sau khi co webhook)."""
    ti = context["task_instance"]
    logging.getLogger("airflow.task").error(
        "[tiki_daily] Task THAT BAI: dag=%s task=%s run=%s try=%s log_url=%s",
        ti.dag_id, ti.task_id, context["run_id"], ti.try_number, ti.log_url,
    )


def refresh_dashboard_callable(**context):
    """Best-effort: goi API Metabase de sync lai schema Postgres, de
    dashboard (khi da dung) thay cot moi ngay khi Gold doi. Metabase CHUA
    duoc cau hinh dashboard/auth that (viec con lai, chua lam) - task nay
    KHONG lam DAG fail neu Metabase chua san sang, chi log ro trang thai."""
    import requests

    try:
        resp = requests.get("http://metabase:3000/api/health", timeout=10)
        logging.getLogger("airflow.task").info(
            "Metabase health check: HTTP %d - %s", resp.status_code, resp.text
        )
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("airflow.task").warning(
            "Khong goi duoc Metabase (co the chua san sang, khong lam DAG fail): %s", exc
        )


with DAG(
    dag_id="tiki_daily",
    description="Crawl Tiki -> validate -> Spark Silver -> Postgres -> dbt -> dashboard",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 24, tz="Asia/Bangkok"),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_callback,
    tags=["tiki"],
) as dag:

    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command=(
            f"cd {REPO} && python3 tiki_crawl.py "
            "--categories all --pages 20 --rps 1.0 "
            "--dt {{ ds }} --bronze-only "
            f"--out {DATA_DIR}"
        ),
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=(
            f"cd {REPO} && python3 validate_bronze.py "
            "--dt {{ ds }} --hour 00 "
            f"--data-dir {DATA_DIR} --min-rows 5000"
        ),
    )

    spark_to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command=(
            f"cd {REPO} && python3 spark/tiki_bronze_to_silver.py "
            "--dt {{ ds }} "
            f"--data-dir {DATA_DIR}"
        ),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {REPO} && python3 load_silver_to_postgres.py --data-dir {DATA_DIR} --verbose",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {REPO}/dbt && dbt snapshot --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {REPO}/dbt && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {REPO}/dbt && dbt test --profiles-dir .",
    )

    refresh_dashboard = PythonOperator(
        task_id="refresh_dashboard",
        python_callable=refresh_dashboard_callable,
    )

    crawl_tiki >> validate_bronze >> spark_to_silver >> load_postgres
    load_postgres >> dbt_snapshot >> dbt_run >> dbt_test >> refresh_dashboard
