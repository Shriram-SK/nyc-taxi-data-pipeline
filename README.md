# NYC Taxi Data Engineering Pipeline

A production-grade data pipeline for NYC Yellow Taxi (2023) data, built with **dbt + DuckDB**, **Apache Airflow**, and **PySpark**. Implements layered SQL transformation, daily orchestration, data quality testing, and analytical query patterns against ~13M monthly trip records.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Data Modeling Approach](#4-data-modeling-approach)
5. [Key Design Decisions](#5-key-design-decisions)
6. [Pipeline Orchestration](#6-pipeline-orchestration)
7. [How to Run](#7-how-to-run)
8. [SQL Analysis](#8-sql-analysis)
9. [Assumptions & Trade-offs](#9-assumptions--trade-offs)
10. [Future Improvements](#10-future-improvements)

---

## 1. Project Overview

This pipeline ingests NYC TLC Yellow Taxi trip records (Parquet format), transforms them through a three-layer dbt model hierarchy in DuckDB, and exposes analytics-ready tables for querying. Airflow schedules and monitors the daily run with per-layer retry granularity.

**What it produces:**

| Output | Description |
|---|---|
| `fct_trips` | Row-level fact table — one row per valid trip |
| `dim_zones` | TLC zone dimension — 255 location IDs with borough and classification |
| `agg_daily_metrics` | Daily rollup: volume, revenue, fare, distance, duration |
| `agg_zone_performance` | Zone-level rollup with revenue rank and high-volume flag |

**Source data:** NYC TLC Yellow Taxi trip records, 2023 (monthly Parquet files) + `taxi_zone_lookup.csv` reference table.

---

## 🚀 Key Highlights

- Built a full end-to-end data pipeline using dbt, DuckDB, and Airflow
- Processes ~150M+ rows efficiently using columnar query execution
- Implements production-grade layering: staging → intermediate → marts
- Handles data quality, null safety, and edge cases explicitly
- Includes orchestration with retries, validation, and dependency control
- Demonstrates analytical SQL patterns (window functions, time-series, ranking)

---

## 2. Architecture

```
  Raw Input Files
  ┌─────────────────────────────────────┐
  │  yellow_tripdata_2023-*.parquet     │  ~13M rows/month, NYC TLC format
  │  taxi_zone_lookup.csv               │  255 zone reference rows
  └──────────────┬──────────────────────┘
                 │
                 ▼  (optional: large historical backfills only)
  ┌──────────────────────────┐
  │  PySpark                 │  spark/process_historical.py
  │  Schema normalisation    │  Column renames, type casts, year/month partitioning
  │  + Parquet partitioning  │  Output: input/processed/year=YYYY/month=MM/
  └──────────────┬───────────┘
                 │
                 ▼
  ┌──────────────────────────────────────────────────────────┐
  │  DuckDB  (nyc_taxi.duckdb)                               │
  │                                                          │
  │  ┌─────────────────────────────────────────────────┐     │
  │  │  STAGING  (views)                               │     │
  │  │  stg_yellow_trips   ← read_parquet() glob       │     │
  │  │  stg_taxi_zones     ← ref(taxi_zone_lookup)     │     │
  │  └───────────────────────┬─────────────────────────┘     │
  │                          │                               │
  │  ┌───────────────────────▼─────────────────────────┐     │
  │  │  INTERMEDIATE  (tables)                         │     │
  │  │  int_trips_enriched  ← LEFT JOIN zones          │     │
  │  │                         + quality filters       │     │
  │  │                         + derived columns       │     │
  │  └───────────────────────┬─────────────────────────┘     │
  │                          │                               │
  │  ┌───────────────────────▼─────────────────────────┐     │
  │  │  MARTS  (tables)                                │     │
  │  │  fct_trips             dim_zones                │     │
  │  │  agg_daily_metrics     agg_zone_performance     │     │
  │  └─────────────────────────────────────────────────┘     │
  └──────────────────────────┬───────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────┐
  │  Apache Airflow  (Docker)            │
  │  DAG: nyc_taxi_pipeline              │
  │                                      │
  │  validate → deps → staging →         │
  │  intermediate → marts → test →       │
  │  export                              │
  └──────────────────────────┬───────────┘
                             │
                             ▼
  ┌──────────────────────────────────────┐
  │  Ad-hoc SQL Queries                  │
  │  queries/q1_top_zones_by_revenue     │
  │  queries/q2_hour_of_day_pattern      │
  │  queries/q3_consecutive_gap_analysis │
  └──────────────────────────────────────┘
```

---

## 3. Tech Stack

| Tool | Version | Why |
|---|---|---|
| **DuckDB** | 1.0.0 | In-process OLAP engine. Reads Parquet natively via `read_parquet()` glob, runs at columnar-scan speeds on a single machine, and requires zero infrastructure. Profile swap to BigQuery or Snowflake is a one-line change. |
| **dbt-duckdb** | 1.8.2 | Brings dbt's layered modeling, lineage, testing, and documentation to DuckDB. Enforces the staging → intermediate → marts contract and makes transformations version-controlled and testable. |
| **Apache Airflow** | 2.9.1 | Industry-standard DAG scheduler. Provides per-task retry, dependency enforcement, execution history, and a UI for monitoring — all without bespoke orchestration code. |
| **PySpark** | 3.5.1 | Used only for historical backfill (multi-year, 100+ GB loads). DuckDB is single-node; Spark distributes I/O across executors when dataset size makes that worthwhile. |
| **Docker Compose** | — | Makes the Airflow stack (scheduler, webserver, postgres) reproducible and portable without a Kubernetes cluster. Mirrors a production deployment topology. |
| **Python** | 3.11 | Input validation, Spark job, Airflow DAGs. |

---

## 4. Data Modeling Approach

The project follows dbt's canonical three-layer pattern. Each layer has a single, well-defined responsibility. No layer reaches backwards to re-read a lower layer's source.

### Staging (`models/staging/` — materialised as **views**)

| Model | Source | Responsibility |
|---|---|---|
| `stg_yellow_trips` | `read_parquet()` glob | Column renames (PascalCase → snake_case), explicit type casts, surrogate key (`trip_id`), `pickup_date`, null-safe `trip_duration_minutes` |
| `stg_taxi_zones` | seed `taxi_zone_lookup` | Column renames, `is_airport` boolean flag, `zone_clean` (lowercased) |

Views are intentional here: staging costs nothing to materialise and always reflects the current Parquet files without stale data risk.

**Surrogate key design:** `trip_id = md5(vendor_id || '-' || pickup_datetime || '-' || pickup_location_id)`. The `'-'` separator prevents cross-field collisions (e.g. vendor=`1`, location=`23` vs vendor=`12`, location=`3`). `coalesce(..., '')` prevents a null component from nullifying the entire hash.

### Intermediate (`models/intermediate/` — materialised as **tables**)

| Model | Joins | Filters |
|---|---|---|
| `int_trips_enriched` | LEFT JOIN `stg_taxi_zones` ×2 (pickup + dropoff) | `trip_distance > 0`, `fare_amount > 0`, `passenger_count > 0`, `trip_duration_minutes BETWEEN 1 AND 180` |

All business logic — joins, quality filters, and derived columns — is concentrated here. Mart models are kept intentionally thin as a result. Added columns: `pickup_zone_name`, `pickup_borough`, `dropoff_zone_name`, `dropoff_borough`, `is_long_trip`, `revenue`, `trip_month`, `trip_speed_mph`.

### Marts (`models/marts/` — materialised as **tables**)

| Model | Grain | Purpose |
|---|---|---|
| `fct_trips` | One row per trip | Core fact table; direct pass-through of `int_trips_enriched` with documented column contract |
| `dim_zones` | One row per TLC location ID | Zone dimension sourced from staging (not intermediate) to guarantee all 255 zones are present regardless of trip coverage |
| `agg_daily_metrics` | One row per `pickup_date` | Daily volume, revenue, fare, distance, duration, and tip rollups |
| `agg_zone_performance` | One row per pickup zone | Zone revenue rank (`RANK()` for tie-safe ordering), tip rate, speed, and `is_high_volume_zone` flag |

`dim_zones` sources from `stg_taxi_zones` directly — not from `int_trips_enriched` — because a dimension table's completeness must be independent of whether trips exist for a given zone in the current period.

---

## 5. Key Design Decisions

### DuckDB over a cloud warehouse

DuckDB reads Parquet files in-place without loading data into a separate storage layer. For a single-year analytical dataset (~150M rows), it achieves sub-second aggregation with no cluster to provision, no cloud bill, and no network round-trip. The `dbt-duckdb` adapter is interface-compatible with `dbt-bigquery` and `dbt-snowflake` — migration is a profile change, not a rewrite.

The `quoting: false` setting in `dbt_project.yml` is a DuckDB-specific requirement: DuckDB lowercases unquoted identifiers at storage time. Leaving quoting enabled causes dbt to emit double-quoted names that DuckDB treats as case-sensitive, producing `relation not found` errors at runtime.

### LEFT JOIN over INNER JOIN for zone enrichment

TLC location IDs 264 ("NV") and 265 ("Unknown") appear in the raw data but have no corresponding zone record. An `INNER JOIN` would silently drop these trips — roughly 0.1% of records that represent real, paid journeys. A `LEFT JOIN` preserves them with `NULL` zone columns, and downstream models `coalesce(pickup_zone_name, 'Unknown')` to keep them visible in aggregations.

### Data quality filters in the intermediate layer, not staging

Staging is a contract with the source: it reflects exactly what the raw data contains, only renamed and retyped. Filtering in staging would make it impossible to audit what was dropped and why. The intermediate layer applies four thresholds, each with an explicit rationale in the SQL:

| Filter | Rationale |
|---|---|
| `trip_distance > 0` | Zero-distance trips are meter tests or cancellations, not completed journeys |
| `fare_amount > 0` | Non-positive fares indicate voids, disputes, or no-charge rate codes |
| `passenger_count > 0` | Driver-entered field; zero means unset, not zero passengers |
| `trip_duration_minutes BETWEEN 1 AND 180` | < 1 min are meter errors; > 3 hrs are extreme outliers that skew averages. NULLs from the staging null-guard fail `BETWEEN` implicitly — no separate `IS NOT NULL` needed |

### Null-safe `trip_duration_minutes`

The staging model uses epoch arithmetic (`(epoch(dropoff) - epoch(pickup)) / 60.0`) rather than `datediff('minute', ...)` to preserve sub-minute precision. The expression is wrapped in a `CASE WHEN ... IS NULL OR dropoff < pickup THEN NULL` guard so that structurally invalid timestamps produce `NULL` rather than a negative or misleading duration. The intermediate layer's `BETWEEN` filter then excludes these rows cleanly.

### `revenue` vs `total_amount`

`total_amount` is aliased as `revenue` in `int_trips_enriched` using `coalesce(total_amount, 0)` to make it null-safe for downstream `SUM()` without requiring repeated null guards. This is a naming convention, not a calculation — the TLC-provided total is never recomputed from components.

---

## 6. Pipeline Orchestration

Two Airflow DAGs coexist in `dags/`. They serve different purposes and are scheduled at different times to avoid DuckDB write-lock contention (DuckDB allows only one writer at a time).

### `nyc_taxi_daily_pipeline.py` — 7-task layer-by-layer DAG

Runs at **06:00 UTC daily**.

```
validate_inputs → dbt_deps → run_dbt_staging → run_dbt_intermediate
                           → run_dbt_marts  → run_dbt_tests → export_reports
```

| Task | Type | Purpose |
|---|---|---|
| `validate_inputs` | PythonOperator | Confirms Parquet files and `taxi_zone_lookup.csv` exist before spending compute on dbt |
| `dbt_deps` | BashOperator | Resolves `packages.yml` — runs every time so package updates take effect without image rebuilds |
| `run_dbt_staging` | BashOperator | `dbt run --select staging` with `ds` date var injected |
| `run_dbt_intermediate` | BashOperator | `dbt run --select intermediate` |
| `run_dbt_marts` | BashOperator | `dbt run --select marts` |
| `run_dbt_tests` | BashOperator | `dbt test` — failed tests mark the DAG run failed, blocking downstream consumers |
| `export_reports` | BashOperator | Placeholder for BI push / notification |

Layers run as **separate tasks** (not a single `dbt run`) so that a failure in `intermediate` doesn't trigger a retry of staging, and the Airflow UI pinpoints exactly which layer failed.

All `BashOperator` dbt commands are prefixed with `set -e`, which causes the shell to exit immediately on any non-zero return code. Without this, a dbt parse warning that exits non-zero would be swallowed and the task would be marked successful.

**Reliability settings (applied to all tasks via `DEFAULT_ARGS`):**

```python
retries          = 2
retry_delay      = timedelta(minutes=5)
execution_timeout= timedelta(hours=1)
max_active_runs  = 1   # DuckDB write-lock guard
catchup          = False
```

### `nyc_taxi_pipeline.py` — 3-task simplified DAG

Runs at **midnight UTC daily** (offset from the above to prevent write-lock collision).

```
dbt_seed → dbt_run → dbt_test
```

Full-graph execution with no layer separation. Suitable for development, CI, or environments where granular retry control is not required.

### Infrastructure

The Airflow stack runs in Docker Compose with four services: `postgres` (metadata DB), `airflow-init` (one-shot migration + admin user creation), `webserver`, and `scheduler`. The `webserver` and `scheduler` declare `airflow-init: condition: service_completed_successfully` in `depends_on` — they will not start until the database is fully migrated.

dbt and DuckDB are installed via `_PIP_ADDITIONAL_REQUIREMENTS` in the Airflow image's entrypoint, avoiding a custom Dockerfile while ensuring all services have the same package versions.

The DuckDB file is persisted on a named Docker volume (`airflow-data`) mounted at `/opt/airflow/data/db`, so it survives `docker compose down` and container restarts.

---

## 7. How to Run

### Prerequisites

| Tool | Version |
|---|---|
| Python | 3.11+ |
| Docker + Docker Compose | 24+ |
| Java (for PySpark backfill only) | 11 or 17 |

### Step 1 — Clone and install dependencies

```bash
git clone <repo-url>
cd nyc-taxi-pipeline

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 2 — Configure the dbt profile

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Set DUCKDB_PATH or accept the default (nyc_taxi.duckdb in the project root)
```

### Step 3 — Download source data

Place monthly Parquet files and the zone lookup in `input/`:

```
input/
  yellow_tripdata_2023-01.parquet
  yellow_tripdata_2023-02.parquet
  ...
  yellow_tripdata_2023-12.parquet
  taxi_zone_lookup.csv
```

Download from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Step 4 — Run dbt locally

```bash
cd dbt

# Install package dependencies
dbt deps

# Load seed reference data
dbt seed

# Run all transformation layers
dbt run

# Run a specific layer only
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Override date range
dbt run --vars '{"start_date": "2023-01-01", "end_date": "2023-03-31"}'

# Override input path (local dev)
dbt run --vars '{"input_dir": "input"}'

# Run data quality tests
dbt test

# Browse lineage and column documentation
dbt docs generate && dbt docs serve
```

### Step 5 — Run Airflow (Docker)

```bash
# Start all services
docker compose up -d

# First boot takes ~60 seconds. Check readiness:
docker compose logs -f airflow-init   # wait for "Admin user created"
docker compose logs -f webserver      # wait for "Airflow is ready"

# Open the UI
open http://localhost:8080            # login: admin / admin

# Enable and trigger the DAG manually
# UI → DAGs → nyc_taxi_pipeline → Enable → Trigger DAG

# Tail scheduler logs
docker compose logs -f scheduler

# Shut down (preserves DuckDB volume)
docker compose down

# Full teardown including data volumes
docker compose down -v
```

### Step 6 — Run ad-hoc queries

```bash
# Against the locally built DuckDB file
duckdb nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
duckdb nyc_taxi.duckdb < queries/q2_hour_of_day_pattern.sql
duckdb nyc_taxi.duckdb < queries/q3_consecutive_gap_analysis.sql
```

### Step 7 — Run the PySpark historical backfill (optional)

Only needed for multi-year loads too large for DuckDB's single-node scan.

```bash
spark-submit spark/process_historical.py \
  --input_dir  input/ \
  --output_dir input/processed/ \
  --year       2023
```

Output is Parquet partitioned by `year` / `month`, compatible with the staging glob pattern.

---

## 8. SQL Analysis

Standalone queries in `queries/` run directly against the DuckDB file against `intermediate.int_trips_enriched`. They demonstrate analytical SQL patterns and answer specific operational questions about the dataset.

### `q1_top_zones_by_revenue.sql` — Top zones by revenue

```sql
-- Pattern: GROUP BY + ORDER BY + LIMIT
SELECT coalesce(pickup_zone_name, 'Unknown'), round(sum(revenue), 2)
FROM intermediate.int_trips_enriched
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

`coalesce` ensures unknown-location trips (IDs 264/265) appear as a labelled group rather than being silently dropped. A plain `GROUP BY pickup_zone_name` would exclude NULLs from the result, under-reporting total revenue.

### `q2_hour_of_day_pattern.sql` — Trip volume by hour

```sql
-- Pattern: generate_series + LEFT JOIN to guarantee all 24 hours present
WITH hours AS (SELECT * FROM generate_series(0, 23))
SELECT h.hour_of_day, coalesce(t.total_trips, 0)
FROM hours h LEFT JOIN trips t ON h.hour_of_day = t.hour_of_day;
```

`generate_series(0, 23)` produces a complete hour spine. The LEFT JOIN ensures hours with zero trips return `0` rather than disappearing from the output — critical for time-series charts where gaps are misleading. DuckDB supports this pattern natively.

### `q3_consecutive_gap_analysis.sql` — Inter-trip gaps per zone

```sql
-- Pattern: LAG() window function to compute time between consecutive events
LAG(pickup_datetime) OVER (
    PARTITION BY coalesce(pickup_zone_name, 'Unknown')
    ORDER BY pickup_datetime
) AS previous_trip_time
```

Partitions by zone (not by vendor) to answer the operational question: *"how long is this zone unserved between trips?"* — which informs driver positioning decisions. The gap is computed using epoch subtraction for fractional-minute precision. Rows where `pickup_datetime < previous_trip_time` return `NULL` via the `CASE WHEN` guard rather than a negative gap.

---

## 9. Assumptions & Trade-offs

| Area | Assumption / Trade-off |
|---|---|
| **Data completeness** | Parquet files are downloaded manually before each run. A production deployment would pull from an S3 bucket or TLC API automatically. |
| **DuckDB concurrency** | DuckDB allows one writer at a time. `max_active_runs=1` on both DAGs prevents write-lock collisions, but this means the pipeline cannot run in parallel. Acceptable for daily batch; not for sub-hourly workloads. |
| **Quality filter thresholds** | The `BETWEEN 1 AND 180` duration filter and `> 0` distance/fare filters are based on TLC documentation and common practice. Edge cases exist (e.g. airport waiting trips) that these may incorrectly exclude. Thresholds are documented in SQL and can be adjusted without code changes. |
| **Zone lookup as a seed** | `taxi_zone_lookup.csv` is loaded as a dbt seed. This is appropriate for a static, 255-row reference table. If TLC frequently updated zone boundaries, this should become a source model instead. |
| **Surrogate key uniqueness** | `trip_id = md5(vendor_id || pickup_datetime || pickup_location_id)` is not guaranteed unique if a vendor logs two trips from the same location at the exact same second. This is extremely rare in practice but would be addressed in production by including a row-level file offset or a TLC-provided trip UUID if available. |
| **Freshness check is a proxy** | `sources.yml` uses `tpep_pickup_datetime` as the `loaded_at_field` for freshness monitoring. This reflects data recency, not file delivery recency. A mtime-based check would require querying file system metadata outside dbt. |
| **Airflow packages at runtime** | `_PIP_ADDITIONAL_REQUIREMENTS` installs dbt/duckdb at container startup. This adds ~60–90 seconds to cold starts. A custom Docker image with packages baked in would eliminate this at the cost of image maintenance overhead. |

---

## Performance Considerations

- DuckDB processes Parquet files using vectorized execution and predicate pushdown, minimizing I/O
- Filtering is applied early (intermediate layer) to reduce downstream compute
- Aggregations operate on pre-cleaned datasets, avoiding repeated validation cost
- Potential partitioning strategy: `pickup_date` or `trip_month` for scaling to multi-year data

---

## 10. Future Improvements

**Observability**
- Add `dbt-expectations` for statistical data quality tests (value range checks, distribution assertions)
- Integrate Airflow alerting (Slack / PagerDuty) on task failure via `on_failure_callback`
- Emit dbt run metadata to a monitoring table for pipeline SLA tracking

**Data coverage**
- Parameterise year in `stg_yellow_trips` so the glob covers multi-year data without SQL changes
- Add `stg_green_trips` and `stg_fhv_trips` to cover the full TLC trip record dataset
- Automate Parquet download from the TLC S3 bucket as a DAG task

**Architecture**
- Add a `generate_schema_name` dbt macro to control schema naming (currently dbt-duckdb may produce `main_intermediate` vs `intermediate` depending on adapter version)
- Replace the manual `dbt_packages/` install with a baked Docker image for faster startup
- Add incremental materialisation to `int_trips_enriched` and `fct_trips` for day-over-day runs that process only new Parquet files

**Testing**
- Add `unique` and `not_null` tests to `stg_yellow_trips.trip_id`
- Add relationship tests between `fct_trips.pickup_location_id` and `dim_zones.location_id`
- Add `accepted_values` tests on `payment_type` and `rate_code_id`
- Add a dbt source freshness alert to the Airflow DAG (`dbt source freshness` as a task)

---

## Project Structure

```
.
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_yellow_trips.sql       # Raw trips: renames, casts, trip_id, duration
│   │   │   ├── stg_taxi_zones.sql         # Zone reference: renames, is_airport flag
│   │   │   └── sources.yml                # Source declarations + freshness config
│   │   ├── intermediate/
│   │   │   └── int_trips_enriched.sql     # Zone join + quality filters + derived cols
│   │   └── marts/
│   │       ├── fct_trips.sql              # Core fact table (pass-through)
│   │       ├── dim_zones.sql              # Zone dimension
│   │       ├── agg_daily_metrics.sql      # Daily rollup
│   │       └── agg_zone_performance.sql   # Zone-level rollup with ranking
│   ├── seeds/                             # taxi_zone_lookup.csv goes here
│   ├── tests/                             # Custom data tests
│   ├── dbt_project.yml                    # Project config, materialisation, vars
│   └── profiles.yml.example              # DuckDB connection template
│
├── dags/
│   ├── nyc_taxi_daily_pipeline.py         # 7-task layer-by-layer DAG (06:00 UTC)
│   └── nyc_taxi_pipeline.py               # 3-task simplified DAG (midnight UTC)
│
├── queries/
│   ├── q1_top_zones_by_revenue.sql        # Top 10 zones by revenue
│   ├── q2_hour_of_day_pattern.sql         # Trip volume by hour (spine guaranteed)
│   └── q3_consecutive_gap_analysis.sql    # Inter-trip gaps per zone (LAG pattern)
│
├── spark/
│   └── process_historical.py             # PySpark backfill for multi-year loads
│
├── input/                                 # Raw Parquet + CSV (gitignored)
├── docker-compose.yml                     # Airflow stack (scheduler, webserver, postgres)
├── requirements.txt                       # Pinned Python dependencies
└── .gitignore                             # Excludes *.duckdb, target/, .venv/, input data
```

---

## Why This Approach Works

This pipeline is designed to balance simplicity, performance, and production-readiness:

- DuckDB enables fast analytics without infrastructure overhead
- dbt enforces modular, testable, and maintainable transformations
- Airflow provides reliable orchestration and observability

The result is a lightweight yet scalable system that mirrors production design patterns while remaining easy to run locally.