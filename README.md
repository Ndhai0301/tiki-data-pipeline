# tiki-crawl

A daily pipeline that tracks **product price movements on Tiki**: crawl → medallion architecture (Bronze / Silver / Gold) → star schema with dbt → BI dashboards (Metabase / Power BI).

Primary goal: record the price of ~20,000 products every day to analyze price trends, promotions, and the history of product attribute changes.

---

## Architecture

```
                          Airflow DAG "tiki_daily" (12:00 noon, daily)
                                        │
  ┌──────────────┐   ┌───────────────┐  │  ┌───────────────┐   ┌──────────────┐   ┌──────────┐
  │  crawl_tiki  │──▶│ validate_bronze│──┼─▶│spark_to_silver│──▶│ load_postgres │──▶│   dbt    │──▶ Metabase / Power BI
  └──────────────┘   └───────────────┘  │  └───────────────┘   └──────────────┘   └──────────┘
     Bronze              quality gate       Silver                raw.listings      staging → marts
   (JSONL.gz)          (fail fast)         (Parquet)             (DuckDB, Postgres)  (star schema, gold)
                                        │
                                        └─▶ compact_silver (merge small Silver files, runs in parallel)
```

| Layer | Format | Location | Role |
|---|---|---|---|
| **Bronze** | JSONL + gzip | `data/bronze/dt=/hour=/category=/` | Immutable raw copy; re-parse if Tiki changes its schema |
| **Silver** | Parquet + snappy | `data/silver/dt=/hour=/category=/` | Typed, deduplicated by `product_id` |
| **Silver compacted** | Parquet | `data/silver_compacted/category=/<YYYY-MM>.parquet` | Small files merged per month (reduces file-open overhead) |
| **Gold** | Postgres tables (schema `gold`) | `tiki_postgres` | Star schema for BI |

---

## Tech stack

- **Crawler**: Python + `curl_cffi` (impersonates Chrome's TLS fingerprint to bypass Tiki's WAF — plain `requests` gets blocked with 403)
- **Bronze→Silver transform**: PySpark (runs inside the Airflow container, no separate cluster)
- **Silver→Postgres EL**: DuckDB (reads Parquet via glob, writes straight to Postgres, full refresh)
- **Gold transform**: dbt (dbt-postgres) — staging views + marts tables + one SCD2 snapshot
- **Orchestration**: Apache Airflow 2.9.3 (LocalExecutor)
- **Data warehouse**: PostgreSQL 16
- **BI**: Metabase (self-hosted) + Power BI Desktop (optional)
- **Infrastructure**: Docker Compose

---

## Directory layout

```
tiki-crawl/
├── tiki_crawl.py              # Crawler: Tiki listing API → Bronze (+ Silver via pandas unless --bronze-only)
├── validate_bronze.py         # Quality gate: fail if Bronze has too few rows / too many empty categories
├── load_silver_to_postgres.py # EL: Silver Parquet → Postgres raw.listings (DuckDB, full refresh)
├── compact.py                 # Merge small Silver files per month → data/silver_compacted/
├── run_pipeline.sh            # Manual EL+T script (not via Airflow) - rarely used
├── spark/
│   ├── schema.py              # Explicit LISTING_SCHEMA for the Bronze JSON
│   ├── transform.py           # Pure bronze_to_silver() function - independently testable
│   └── tiki_bronze_to_silver.py  # Spark job: Bronze → Silver (the official job in the DAG)
├── dags/
│   └── tiki_daily.py          # Airflow DAG - the main pipeline
├── dbt/
│   ├── models/
│   │   ├── staging/           # stg_listings, stg_price_readings
│   │   └── marts/             # dim_*, fact_price_daily, fact_price_change
│   ├── snapshots/
│   │   └── dim_product_snapshot.sql  # SCD Type 2 for dim_product
│   ├── dbt_project.yml
│   └── profiles.yml
├── airflow/
│   └── Dockerfile             # apache/airflow + Java 17 (PySpark) + toolchain (duckdb, dbt, curl_cffi...)
├── docker-compose.yml
├── docs/                      # Design docs (data model, ERD, storage layout, API schema)
├── samples/                   # Sample JSONL for testing transforms without crawling
└── tests/
    └── test_transform.py      # Unit tests for spark/transform.py
```

---

## Running the project

### Start

```bash
cd ~/tiki-crawl
docker compose up -d
```

Check that all 5 containers are `Up`:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep tiki
```

### Access points

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8081 | `admin` / `admin` |
| Metabase | http://localhost:3001 | (set up on first visit) |
| Postgres (data warehouse) | `localhost:5433` | db `tiki`, user/pass `tiki` / `tiki` |
| Postgres (Airflow metadata) | internal, not exposed | — |

> Port **5433** (not 5432) to avoid clashing with `tiki_airflow_postgres`.

### Run the pipeline manually (without waiting for noon)

```bash
docker exec tiki_airflow_scheduler airflow dags trigger tiki_daily
```

### Stop

```bash
docker compose down        # keeps data (volumes)
docker compose down -v     # CAUTION: also deletes volumes (loses Postgres/Metabase data)
```

---

## Pipeline (`tiki_daily` DAG)

- **Schedule**: `0 12 * * *` (12:00 noon, `Asia/Bangkok`)
- **`catchup=False`**: does NOT backfill missed days — Tiki only returns the current price, so a late run would write the wrong price into an old date
- **`max_active_runs=1`**: DAG runs execute sequentially (dbt writes to one shared Postgres schema, not safe to run concurrently)

| Task | What it does |
|---|---|
| `crawl_tiki` | `tiki_crawl.py --categories all --pages 20 --dt {{ ds }} --hour 12 --bronze-only` |
| `validate_bronze` | Fail if Bronze has < 5000 rows or > 3 empty categories |
| `spark_to_silver` | Spark job: Bronze → Silver Parquet |
| `load_postgres` | DuckDB: Silver → `raw.listings` (full refresh, `DROP TABLE ... CASCADE`) |
| `compact_silver` | Merge the current month's small Silver files (runs in parallel with `load_postgres`) |
| `dbt_snapshot` | `dbt run --select staging && dbt snapshot` (rebuilds the staging view first, since `load_postgres` just dropped it via CASCADE) |
| `dbt_run` | Build all gold models |
| `dbt_test` | 32 data tests |
| `refresh_dashboard` | Ping Metabase health (best-effort, never fails the DAG) |

---

## Data model (schema `gold`)

Star schema — see [docs/erd.md](docs/erd.md) for details.

```
        dim_date ──┐                  ┌── dim_brand   (currently EMPTY: Tiki listing API returns no brand_id)
                   ├── fact_price_daily ├── dim_seller
        dim_product┤                  └── dim_category
                   └── fact_price_change
```

| Table | Grain | Notes |
|---|---|---|
| `dim_date` | 1 day | Date spine 2020-2035, pre-built, independent of the crawl |
| `dim_product` | 1 product version | **SCD Type 2** (source: `dim_product_snapshot`), has `valid_from` / `valid_to` / `is_current` |
| `dim_brand` / `dim_seller` / `dim_category` | 1 value | Surrogate key = md5 hash of the natural key |
| `fact_price_daily` | `(date_key, hour, product_key)` | 1 row = a price observed in one crawl |
| `fact_price_change` | `(product_key, changed_at)` | 1 row = one **actual price change** vs the previous crawl |

---

## Operations — important notes

- **The machine must be on (WSL + Docker running) around 12:00 noon each day.** If it sleeps/shuts down at that moment, that day is skipped and is not backfilled (by design). A clean run takes ~6-7 minutes; allow a ~15-20 minute buffer.
- **The host cron job is DISABLED** — Airflow is the sole data source. Do not re-enable a parallel cron (it would overwrite Airflow's output).
- **Airflow containers run as `user: "1000:0"`** (matching the host user's UID) — prevents permission conflicts when both the host and the container write into the bind-mounted directories (`data/`, `dbt/`).
- **Date label is off by one day**: Airflow sets `ds` to the start of the interval, so data crawled today at noon is written to the `dt=<yesterday>` partition. This is normal Airflow behavior, not a bug.
- **`dim_brand` is empty**: the Tiki listing API does not return `brand_id` (only the detail API does). To analyze by brand, `tiki_crawl.py` would need to also call the detail API — the tradeoff is a much slower crawl.

---

## Reference docs

- [docs/data-model.md](docs/data-model.md) — data model design decisions
- [docs/erd.md](docs/erd.md) — detailed star schema ERD
- [docs/storage.md](docs/storage.md) — partition conventions, compaction
- [docs/tiki_api_schema.md](docs/tiki_api_schema.md) — Tiki API response schema
