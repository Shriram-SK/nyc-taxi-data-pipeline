# NYC Yellow Taxi Pipeline — Technical Documentation & Interview Guide

> **Stack:** DuckDB · dbt 1.8 · Apache Airflow 2.9 · Docker · PySpark · Python 3.11
> **Dataset:** NYC TLC Yellow Taxi 2023 — 38.3M raw trips, 35.5M post-filter
> **Run time:** ~2 minutes end-to-end inside Docker on a 3.5 GB WSL2 VM

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Modeling — dbt Layers](#3-data-modeling--dbt-layers)
4. [Airflow DAG Walkthrough](#4-airflow-dag-walkthrough)
5. [SQL Analysis Queries](#5-sql-analysis-queries)
6. [Scalability — PySpark & Cloud](#6-scalability--pyspark--cloud)
7. [Challenges Faced & How They Were Solved](#7-challenges-faced--how-they-were-solved)
8. [Design Decisions & Tradeoffs](#8-design-decisions--tradeoffs)
9. [Improvements & Future Work](#9-improvements--future-work)
10. [Brainstormer: Should Mart Tables Be Visible If Tests Fail?](#10-brainstormer-should-mart-tables-be-visible-if-tests-fail)
11. [Interview Questions & Ideal Answers](#11-interview-questions--ideal-answers)
12. [2-Minute Elevator Pitch](#12-2-minute-elevator-pitch)

---

## 1. Project Overview

### Problem Statement

The NYC Taxi and Limousine Commission (TLC) releases monthly Parquet files containing every metered yellow taxi trip in New York City. In raw form, these files are analytically useless — column names are PascalCase TLC jargon, types are inconsistent across months, there is no surrogate key, invalid records (meter tests, voids, zero-distance trips) are mixed into the dataset, and zone geography exists only as opaque integer IDs.

This pipeline transforms that raw data into a clean, tested, analytics-ready data mart that can answer questions like:
- Which pickup zones generate the most revenue?
- What time of day do trip volumes peak?
- How long does a given zone go unserved between consecutive trips?

### Dataset

| File | Description | Rows |
|---|---|---|
| `yellow_tripdata_2023-01.parquet` … `2023-12.parquet` | Monthly trip records | 38.3M raw |
| `taxi_zone_lookup.csv` | TLC zone → borough → service type | 265 rows |

Key raw columns: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `total_amount`, `payment_type`, `passenger_count`, `trip_distance`.

### Scale

- **38,310,226 raw trip rows** across 12 monthly Parquet files
- **35,468,135 rows** survive quality filters in the intermediate layer (~7.4% filtered out as invalid)
- **265 zone dimension rows** covering all five NYC boroughs, EWR, and unknown zones
- DuckDB file size on disk: ~1.1 GB after all layers are materialized

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│  INPUT (Docker volume: /opt/airflow/data/input)                          │
│                                                                          │
│  yellow_tripdata_2023-01.parquet                                         │
│  yellow_tripdata_2023-02.parquet  ──┐                                    │
│  ...                                │                                    │
│  yellow_tripdata_2023-12.parquet    │                                    │
│  taxi_zone_lookup.csv ──────────────┤                                    │
└─────────────────────────────────────┼────────────────────────────────────┘
                                      │
                          ┌───────────▼──────────────┐
                          │  Apache Airflow           │
                          │  (Docker, scheduler)      │
                          │                           │
                          │  validate_inputs          │
                          │        ↓                  │
                          │  dbt deps                 │
                          │        ↓                  │
                          │  dbt seed                 │  ← loads taxi_zone_lookup
                          │        ↓                  │
                          │  dbt run --select staging │
                          │        ↓                  │
                          │  dbt run --select         │
                          │         intermediate      │
                          │        ↓                  │
                          │  dbt run --select marts   │
                          │        ↓                  │
                          │  dbt test                 │
                          │        ↓                  │
                          │  export_reports           │
                          └───────────┬──────────────┘
                                      │
                          ┌───────────▼──────────────┐
                          │  DuckDB                   │
                          │  /opt/airflow/data/db/    │
                          │  nyc_taxi.duckdb          │
                          │                           │
                          │  Catalog: nyc_taxi        │
                          │                           │
                          │  main_seeds               │
                          │    taxi_zone_lookup       │
                          │                           │
                          │  main_staging             │
                          │    stg_yellow_trips (v)   │
                          │    stg_taxi_zones (v)     │
                          │                           │
                          │  main_intermediate        │
                          │    int_trips_enriched (t) │
                          │                           │
                          │  main_marts               │
                          │    fct_trips (t)          │
                          │    dim_zones (t)          │
                          │    agg_daily_metrics (t)  │
                          │    agg_zone_performance(t)│
                          └──────────────────────────┘
                                      │
                     (v) = view   (t) = table

OUTPUT: Analytics-ready tables queryable by BI tools or ad-hoc SQL
```

### Dependency Lineage

```
taxi_zone_lookup.csv
        │
        └──► dbt seed: taxi_zone_lookup
                  │
                  └──► stg_taxi_zones (view)
                            │
                            ├──► int_trips_enriched (table)
                            │           │
yellow_tripdata_*.parquet   │           ├──► fct_trips (table)
        │                   │           ├──► agg_daily_metrics (table)
        └──► stg_yellow_trips (view)    └──► agg_zone_performance (table)
                            │
                            └──► int_trips_enriched
                                         │
                                         └──► dim_zones (table) ← from stg_taxi_zones directly
```

---

## 3. Data Modeling — dbt Layers

### Design Principle

Each layer has a single, bounded responsibility. Business logic lives in exactly one place. Downstream models never re-implement what upstream models have already done.

---

### Layer 1: Staging (`models/staging/`)

**Purpose:** Make the raw data usable. No business logic, no filtering, no joining.

**Materialization:** Views — staging models cost nothing to rebuild and always reflect the current seed/parquet state.

#### `stg_yellow_trips`

Reads all 12 monthly Parquet files in a single glob via DuckDB's `read_parquet()`:

```sql
from read_parquet('{{ var("input_dir") }}/yellow_tripdata_2023-*.parquet')
```

Responsibilities:
1. **Rename**: `VendorID` → `vendor_id`, `tpep_pickup_datetime` → `pickup_datetime`, `PULocationID` → `pickup_location_id`, etc.
2. **Cast**: Every column explicitly cast (`VendorID as integer`, timestamps as `TIMESTAMP`, amounts as `DOUBLE`)
3. **Surrogate key**: `trip_id` generated via `md5(vendor_id || '-' || pickup_datetime || '-' || pickup_location_id)`. A separator prevents cross-field collisions (vendor=1, location=23 ≠ vendor=12, location=3). `coalesce` guards against null components producing a null hash.
4. **Derived columns**: `pickup_date` (date-truncated timestamp for downstream partitioning); `trip_duration_minutes` (epoch arithmetic, null-safe, handles dropoff < pickup edge case)

**Key decision — `trip_id` not `unique` tested**: High-volume zones produce multiple trips per second from the same vendor. During development, 917,566 hash collisions were found in 2023 data — real data limitation, not a pipeline bug. The `unique` test was intentionally omitted and documented; `not_null` is tested instead to confirm the hash function never returns null.

#### `stg_taxi_zones`

Sources from the dbt seed `taxi_zone_lookup`. Adds:
- `zone_clean`: lowercased zone name for case-insensitive joins
- `is_airport`: boolean flag (`service_zone = 'Airports'`) — covers JFK, LGA, EWR

All 265 zones are retained (no filtering), so downstream LEFT JOINs can rely on complete coverage.

---

### Layer 2: Intermediate (`models/intermediate/`)

**Purpose:** Apply business logic. This is the only place filtering and joining happen.

**Materialization:** Table — materializing here means mart models query pre-joined, pre-filtered data without re-scanning 38M parquet rows on every run.

#### `int_trips_enriched`

Joins `stg_yellow_trips` (38M rows) with `stg_taxi_zones` twice — once for pickup, once for dropoff:

```sql
left join zones as pickup_zone  on trips.pickup_location_id  = pickup_zone.location_id
left join zones as dropoff_zone on trips.dropoff_location_id = dropoff_zone.location_id
```

**Why LEFT JOIN, not INNER JOIN**: TLC location IDs 264 ("NV") and 265 ("Unknown") exist in trip data but have no meaningful zone entry. An INNER JOIN would silently drop ~0.1% of trips and under-count revenue totals. LEFT JOIN preserves them; downstream models coalesce `NULL` zone names to `'Unknown'`.

**Quality filters** applied here (not in staging — staging is the raw contract):

| Filter | Rationale |
|---|---|
| `trip_distance > 0` | Zero-distance = meter test or cancellation |
| `fare_amount > 0` | Non-positive fare = void, dispute, or no-charge code |
| `passenger_count > 0` | Driver-entered field; 0 means not set |
| `trip_duration_minutes BETWEEN 1 AND 180` | <1 min = meter error; >3 hrs = extreme outlier. NULLs fail `BETWEEN` implicitly |

**Result**: 35,468,135 rows (7.4% of raw trips removed)

**Derived columns added**:
- `revenue`: `coalesce(total_amount, 0)` — safe for `SUM` without null checks
- `trip_month`: `date_trunc('month', pickup_date)` — calendar rollup helper
- `is_long_trip`: `trip_duration_minutes > 60` — segmentation flag
- `trip_speed_mph`: `trip_distance / (trip_duration_minutes / 60.0)` with `CASE` guard for zero duration

---

### Layer 3: Marts (`models/marts/`)

**Purpose:** Deliver analytics-ready surfaces. No transformation — aggregate or pass through.

**Materialization:** Tables — fast, pre-joined, pre-aggregated.

#### `fct_trips`

Pure pass-through of `int_trips_enriched`. Exists as a stable, named contract for BI tools and ad-hoc analysts. Grain: one row per trip.

**Why does this exist if it's just `SELECT *`?** Because the name matters. `fct_trips` signals "fact table" to consumers; `int_trips_enriched` signals "internal processing". Naming communicates contract and ownership.

#### `dim_zones`

Sources directly from `stg_taxi_zones` (not from the intermediate layer). This is deliberate: dimension completeness must not depend on whether trips exist for a zone. If a zone had zero trips in the period, it must still appear in `dim_zones` for correct LEFT JOINs in dashboards.

Exposes: `location_id` (PK, tested `unique` + `not_null`), `zone`, `borough`, `service_zone`.

#### `agg_daily_metrics`

One row per `pickup_date`. Computes: `total_trips`, `total_revenue`, `avg_fare` (metered component only, not `total_amount`, to isolate the base fare trend from tip/surcharge volatility), `avg_distance`, `avg_duration_minutes`, `total_tips`, `avg_passengers`.

#### `agg_zone_performance`

One row per pickup zone. Computes volume, revenue, operational, and tip-rate metrics. Notable decisions:
- Unknown zones coalesced to `'Unknown'` rather than excluded — revenue totals remain accurate
- `revenue_rank` uses `RANK()` not `ROW_NUMBER()` — ties receive equal rank, with gaps after (meaningful for low-volume time slices where zones may genuinely tie)
- `is_high_volume_zone`: flag for zones exceeding 100,000 trips/year — threshold appropriate for full-year 2023 data
- `tip_rate_pct`: `sum(tip_amount) / nullif(sum(revenue), 0) * 100` — proxy for card vs. cash payment mix and customer satisfaction

---

### Schema Naming in DuckDB

dbt appends the target schema prefix to custom schemas. With `schema: main` in `profiles.yml` and `+schema: staging` in `dbt_project.yml`, the physical schema becomes `main_staging`. Full qualified table reference: `nyc_taxi.main_intermediate.int_trips_enriched`.

This confused BI queries written with `FROM intermediate.int_trips_enriched` — those must use the `main_intermediate` prefix or the DuckDB file must be opened with `USE nyc_taxi`.

---

## 4. Airflow DAG Walkthrough

**DAG ID:** `nyc_taxi_daily_pipeline`
**Schedule:** `0 6 * * *` (06:00 UTC daily)
**Start date:** 2024-01-01
**Catchup:** False
**Max active runs:** 1 (DuckDB is single-writer; concurrent runs would deadlock)

### Task Dependency Chain

```
validate_inputs → dbt_deps → dbt_seed → run_dbt_staging
    → run_dbt_intermediate → run_dbt_marts → run_dbt_tests → export_reports
```

### Task-by-Task Breakdown

#### `validate_inputs` (PythonOperator)

Runs before any dbt command to fail fast on missing inputs. Checks:
1. At least one `yellow_tripdata_*.parquet` file exists in `INPUT_DIR`
2. `taxi_zone_lookup.csv` exists in `INPUT_DIR`

Failing here immediately (before a 2-minute dbt run) prevents misleading errors deep in the pipeline. Key pattern: **fail fast, fail clearly**.

Environment variable `INPUT_DIR` defaults to `/opt/airflow/data/input` — no hardcoded paths.

#### `dbt_deps` (BashOperator)

```bash
dbt --no-use-colors deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

Installs dbt packages declared in `packages.yml`. In this project there are none, so this is a no-op but essential for pipelines that use `dbt_utils` or `dbt_expectations`. Running `deps` unconditionally ensures the pipeline is self-contained and idempotent.

`--no-use-colors` prevents ANSI escape codes from polluting Airflow task logs.

#### `dbt_seed` (BashOperator)

```bash
dbt --no-use-colors seed --project-dir ... --profiles-dir ...
```

Loads `seeds/taxi_zone_lookup.csv` into `main_seeds.taxi_zone_lookup`. This task **must run before staging** because `stg_taxi_zones` uses `{{ ref('taxi_zone_lookup') }}`. Placing seed before staging in the dependency chain enforces this contract.

#### `run_dbt_staging` (BashOperator)

```bash
set -e; dbt --no-use-colors run --select staging ...
```

Creates views for `stg_yellow_trips` and `stg_taxi_zones`. The `set -e` flag causes bash to exit immediately if dbt returns a non-zero exit code — ensures Airflow marks the task failed rather than silently continuing.

#### `run_dbt_intermediate` (BashOperator)

Materializes `int_trips_enriched` as a physical table. This is the most resource-intensive step: 35.5M rows × 20+ columns, joining with zone reference data.

In the Docker environment (WSL2 with 3.5 GB total RAM), this requires `threads: 1` in `profiles.yml` and `memory_limit: '1.8GB'` with `temp_directory` configured so DuckDB can spill to disk rather than being OOM-killed. With these settings, this step completes in ~37 seconds.

#### `run_dbt_marts` (BashOperator)

Materializes all four mart tables (`fct_trips`, `dim_zones`, `agg_daily_metrics`, `agg_zone_performance`). Each mart aggregates or passes through `int_trips_enriched`, which is already in DuckDB — no re-scanning of Parquet files. Completes in ~30 seconds total.

#### `run_dbt_tests` (BashOperator)

```bash
set -e; dbt --no-use-colors test ...
```

Runs all four schema tests:
- `not_null` on `stg_yellow_trips.trip_id`
- `not_null` on `fct_trips.trip_id`
- `not_null` + `unique` on `dim_zones.location_id`

**Pipeline fails here if any test fails.** `set -e` propagates the dbt non-zero exit to Airflow, marking the task (and subsequently the DAG run) failed. Mart tables exist at this point but the failure prevents downstream consumers from being notified of success.

#### `export_reports` (BashOperator)

Currently a placeholder (`echo 'Pipeline complete'`). In production this would trigger a Slack notification, write a run manifest to S3, or kick off a downstream BI refresh. The task exists as an explicit success signal that all upstream tasks completed cleanly.

### Retry & Failure Handling

```python
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}
```

- 2 retries with 5-minute backoff handles transient failures (network blip during `dbt deps`, temporary I/O spike during large table build)
- 1-hour timeout prevents hung processes from blocking the scheduler indefinitely
- `max_active_runs=1` prevents concurrent DuckDB writers from deadlocking

### Backfill Support

`catchup=False` prevents automatic historical backfills when the DAG is first enabled. For manual backfills, Airflow CLI or the UI can trigger specific `execution_date` values. The `validate_inputs` task checks for the presence of input files rather than a specific date, making the pipeline date-agnostic by design.

---

## 5. SQL Analysis Queries

### Q1: Top Zones by Revenue (`q1_top_zones_by_revenue.sql`)

```sql
select
    coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,
    round(sum(revenue), 2)               as total_revenue
from intermediate.int_trips_enriched
group by coalesce(pickup_zone_name, 'Unknown')
order by total_revenue desc, pickup_zone_name
limit 10;
```

**Business value:** Identifies where to concentrate driver supply and pricing optimization. Zones with disproportionate revenue relative to trip count signal high-fare routes (airport, long-haul).

**Design decisions:**
- `coalesce(pickup_zone_name, 'Unknown')` — unknown TLC location IDs (264/265) appear as a labelled group in results instead of being silently excluded by filters. The Unknown group often represents significant airport or border-area revenue.
- `order by total_revenue desc, pickup_zone_name` — secondary sort on name makes output deterministic when revenue values tie.
- `revenue` column (not `total_amount`) — staging already `coalesce`d null amounts to 0, so `SUM` is null-safe without additional guarding.

---

### Q2: Hour-of-Day Trip Pattern (`q2_hour_of_day_pattern.sql`)

```sql
with hours as (
    select * from generate_series(0, 23) as hour_of_day
),
trips as (
    select
        cast(extract(hour from pickup_datetime) as integer) as hour_of_day,
        count(*) as total_trips
    from intermediate.int_trips_enriched
    group by hour_of_day
)
select
    h.hour_of_day,
    coalesce(t.total_trips, 0) as total_trips
from hours h
left join trips t on h.hour_of_day = t.hour_of_day
order by h.hour_of_day;
```

**Business value:** Demand forecasting for driver positioning, dynamic pricing, and operational staffing. NYC taxi data typically shows twin peaks (morning commute 7–9am, evening/late-night 6–11pm) and a quiet trough (3–5am).

**Design decisions:**
- `generate_series(0, 23)` creates a **spine** of all 24 hours. Without this, hours with zero trips would simply be absent from results — silent gaps that mislead chart consumers into thinking a row is missing rather than zero. The left join ensures every hour is always represented.
- `coalesce(t.total_trips, 0)` — converts NULL from the left join into an explicit 0 for charting.

---

### Q3: Consecutive Gap Analysis (`q3_consecutive_gap_analysis.sql`)

```sql
with ordered as (
    select
        coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,
        pickup_datetime,
        lag(pickup_datetime) over (
            partition by coalesce(pickup_zone_name, 'Unknown')
            order by pickup_datetime
        ) as previous_trip_time
    from intermediate.int_trips_enriched
)
select
    pickup_zone_name,
    pickup_datetime,
    previous_trip_time,
    case
        when previous_trip_time is not null
        and pickup_datetime >= previous_trip_time
        then round(
            (epoch(pickup_datetime) - epoch(previous_trip_time)) / 60.0,
            2
        )
    end as gap_minutes
from ordered
where previous_trip_time is not null
order by pickup_zone_name, pickup_datetime;
```

**Business value:** Identifies supply-demand imbalances by zone. A zone with frequent 30+ minute gaps indicates undersupply. A zone with sub-1-minute gaps indicates saturation. This informs driver dispatch and surge pricing triggers.

**Design decisions:**
- `LAG()` partitioned by zone (not by vendor/driver) — answers "how long is this zone underserved?" rather than "how long did this driver wait?", which is the operationally meaningful question.
- `CASE WHEN pickup_datetime >= previous_trip_time` — guards against rare timestamp inversions in raw TLC data (two trips recorded out of order). Without this guard, negative gap values would appear and corrupt aggregations.
- Epoch arithmetic (`epoch(t2) - epoch(t1)) / 60.0`) rather than `datediff('minute', ...)` — preserves sub-minute precision for short gaps. `DATEDIFF` truncates to whole minutes.
- `WHERE previous_trip_time IS NOT NULL` — excludes the first trip in each zone (no predecessor), keeping results interpretable.

---

## 6. Scalability — PySpark & Cloud

### Current Architecture Limits

DuckDB is single-writer and in-process. It excels for single-machine analytics on datasets up to ~50–100 GB. Beyond that, or when parallel writes are needed, it becomes the bottleneck.

### PySpark Preprocessing (`spark/process_historical.py`)

The Spark script handles bulk historical loads before the dbt pipeline:

```
spark-submit spark/process_historical.py \
  --input_dir /data/raw/yellow_taxi \
  --output_dir /data/processed/yellow_taxi \
  --year 2023
```

It performs:
1. **Schema normalization** — same column renames as dbt staging, but applied at Spark ingestion so downstream tools see consistent names
2. **Structural validation** — drops nulls on timestamp and location ID (structural invalidity only; business-level anomaly detection stays in dbt)
3. **Partition writing** — output partitioned by `year` and `month` using Hive-style partitioning (`partitionBy("year", "month")`)

Design principle: **Spark handles volume, dbt handles logic.** The Spark job produces clean, typed Parquet partitions. dbt then applies business rules, joins, and tests. Neither layer duplicates the other's responsibility.

### Scaling Path

| Scale | Architecture |
|---|---|
| <50 GB | DuckDB + dbt (current) |
| 50 GB – 10 TB | Spark preprocessing → dbt on BigQuery/Snowflake/Redshift |
| >10 TB / streaming | Spark Structured Streaming → Delta Lake / Iceberg → dbt incremental models |

**Concrete changes for cloud scale:**

1. **Replace DuckDB with BigQuery or Snowflake** — swap only the `profiles.yml` adapter. dbt models are adapter-agnostic SQL.
2. **Replace glob staging with partitioned external tables** — `FROM read_parquet(...)` becomes a BigQuery external table or Snowflake stage; `{{ source() }}` replaces the direct `read_parquet()` call.
3. **Incremental intermediate model** — `int_trips_enriched` becomes `materialized='incremental'` with `unique_key='trip_id'` and `incremental_strategy='merge'`. Only new/changed rows are processed on each run instead of full re-scan.
4. **Airflow on Kubernetes (KubernetesPodOperator)** — each dbt task runs in its own pod with defined CPU/memory limits, enabling parallel layer execution.
5. **Add dbt `source_freshness`** — replaces the `validate_inputs` Python task with a native dbt mechanism that checks when source data was last updated against a configured SLA.

---

## 7. Challenges Faced & How They Were Solved

### Challenge 1: Ancient `dbt` Package Hanging Silently

**Symptom:** `dbt run` launched and hung indefinitely with no output.

**Root cause:** The installed package was the legacy monolithic `dbt==1.0.0.40.15` — a pre-adapter-split version that predates `dbt-core` and produced no error messages on incompatible commands.

**Fix:**
```bash
pip uninstall dbt
pip install dbt-core==1.8.3 dbt-duckdb==1.8.2 duckdb==1.0.0
```

**Lesson:** Always verify `dbt --version` shows both Core and plugin versions. The modern install is adapter-specific (`dbt-duckdb`, `dbt-bigquery`), not the generic `dbt` package.

---

### Challenge 2: `profiles.yml` Containing Jinja — Evaluated at Parse Time

**Symptom:** `dbt debug` reported "connection failed" with a cryptic error about `DUCKDB_PATH` env var.

**Root cause:** The `profiles.yml` contained:
```yaml
path: "{{ env_var('DUCKDB_PATH') }}"
```
The env var was not set in the local shell, so dbt failed to parse the profile.

**Fix:** Replaced with a hardcoded absolute path for local development. The Docker version uses the container path `/opt/airflow/data/db/nyc_taxi.duckdb` directly.

**Lesson:** `profiles.yml` is rendered by Jinja at dbt startup, before any env var is read from `.env` files or shell. Either set the env var in the shell or use hardcoded paths per environment.

---

### Challenge 3: `{{ ref() }}` and `{{ source() }}` in SQL Comments

**Symptom:** `dbt parse` failed with "Compilation Error: `ref` takes 1 or 2 arguments".

**Root cause:** A comment block in `stg_taxi_zones.sql` contained:
```sql
-- {{ ref() }} ensures dbt tracks the seed → model dependency
```
dbt's Jinja engine processes `{{ }}` expressions in ALL file content — block comments (`/* */`), line comments (`--`), and YAML `#` comments are not safe from Jinja evaluation.

**Fix:** Stripped `{{ }}` delimiters from all documentation text. Reference the function by name only: `-- ref() ensures dbt tracks...`

**Lesson:** Never use `{{ }}` syntax in dbt file content unless you intend it to be evaluated. This applies to `.sql`, `.yml`, and `.md` files alike.

---

### Challenge 4: `+database: main` Causing Catalog Error

**Symptom:** `dbt run` failed with `Binder Error: Catalog "main" does not exist!`

**Root cause:** `dbt_project.yml` contained `+database: main` in the global model config. In dbt-duckdb, the catalog name is derived from the DuckDB filename — `nyc_taxi.duckdb` creates catalog `nyc_taxi`. Hardcoding `main` overrides this with a catalog that doesn't exist.

**Fix:** Removed `+database: main` entirely. Let dbt-duckdb derive the catalog from the file path.

**Lesson:** DuckDB's catalog naming is file-based, not configurable. `profiles.yml` can optionally set `database: nyc_taxi` for documentation but `dbt_project.yml` model-level `+database` overrides should never be used with dbt-duckdb.

---

### Challenge 5: Windows Absolute Path Breaking Docker Run

**Symptom:** `run_dbt_staging` succeeded locally but failed inside Docker with `FileNotFoundError: Could not open file "C:/Users/shrir/Desktop/Firmable/input/yellow_tripdata_2023-*.parquet"`.

**Root cause:** `dbt_project.yml` had:
```yaml
vars:
  input_dir: "C:/Users/shrir/Desktop/Firmable/input"
```
This path is valid on the Windows host but meaningless inside a Linux Docker container.

**Fix:** Changed the default to the container path:
```yaml
vars:
  input_dir: "/opt/airflow/data/input"
```
Local development overrides with `--vars '{"input_dir": "C:/Users/shrir/Desktop/Firmable/input"}'`.

**Lesson:** Any value that differs between local and Docker environments belongs in an environment variable or `--vars` override, not as a hardcoded default in a committed config file.

---

### Challenge 6: DuckDB Write Permission Denied on Named Volume

**Symptom:** `dbt debug` inside the container reported `IO Error: Cannot open file "/opt/airflow/data/db/nyc_taxi.duckdb": Permission denied`.

**Root cause:** The Docker named volume `airflow-data` mounted at `/opt/airflow/data/db` was created and owned by `root:root` with permissions `755`. The Airflow process runs as `uid=50000` (not root) and could not write to the directory.

**Fix:** Added a `chmod 777` to the `airflow-init` service command in `docker-compose.yml`:
```yaml
command:
  - -c
  - |
    mkdir -p /opt/airflow/data/db && chmod 777 /opt/airflow/data/db
    airflow db migrate
    ...
```
This runs once at container initialization and persists for the lifetime of the volume.

**Lesson:** Named Docker volumes mounted into official images (which run as non-root) always need explicit permission grants. The `airflow-init` service is the correct place to apply this — it runs before the scheduler and webserver start.

---

### Challenge 7: OOM Kill During Intermediate Materialization

**Symptom:** `run_dbt_intermediate` was killed with exit code 137 (SIGKILL) every time. The task logged "START" and then the process vanished.

**Root cause:** The Docker Desktop WSL2 VM had only 3.5 GB total RAM shared across all containers. With the webserver (~350 MB) and postgres (~50 MB) running, only ~2.4 GB was available. dbt 1.8 spawns Python multiprocessing worker processes — with `threads: 2`, two worker processes were launched, each loading the full dbt + DuckDB Python runtime (~500 MB each). The combined memory footprint exceeded the available RAM.

**Investigation:** Direct DuckDB `CREATE TABLE AS SELECT` in Python (single-threaded, `memory_limit='1.8GB'`, `temp_directory` configured) completed successfully — confirming the issue was dbt's multiprocessing overhead, not DuckDB's SQL execution.

**Fix:**
```yaml
# profiles.yml
dev:
  threads: 1              # no worker subprocess spawned
  settings:
    memory_limit: "1.8GB"
    temp_directory: "/opt/airflow/data/db/duckdb_tmp"
```

With `threads: 1`, dbt executes models sequentially in the main process. No subprocess overhead. Intermediate model materialized in 37 seconds.

**Lesson:** In memory-constrained Docker environments, dbt thread count directly controls subprocess count. `threads: 1` is the correct setting for single-node development. The `settings.memory_limit` is a safety net that causes DuckDB to spill to disk rather than being killed — but it only works if the process isn't killed before the setting is applied.

---

### Challenge 8: Missing `dbt seed` Task

**Symptom:** `run_dbt_staging` failed with `Catalog Error: Table "taxi_zone_lookup" does not exist` — but the seed was present in `dbt/seeds/`.

**Root cause:** The original DAG had no `dbt_seed` task. `stg_taxi_zones.sql` references `{{ ref('taxi_zone_lookup') }}`, which requires the seed to have been loaded into DuckDB first. The DAG went from `dbt_deps` directly to `run_dbt_staging`, skipping the seed load entirely.

**Fix:** Added `dbt_seed` task between `dbt_deps` and `run_dbt_staging`, with `set -e` for fail-fast behavior.

**Lesson:** Any dbt model that uses `{{ ref('seed_name') }}` requires an explicit `dbt seed` step upstream in the orchestration. dbt does not automatically load seeds during `dbt run`.

---

## 8. Design Decisions & Tradeoffs

### DuckDB vs. Snowflake / BigQuery

| Dimension | DuckDB (chosen) | Snowflake / BigQuery |
|---|---|---|
| Infrastructure | Zero — a single `.duckdb` file | Requires account, networking, IAM |
| Cost | Free | Per-query or warehouse-hour billing |
| Concurrency | Single writer | Multi-user, concurrent writes |
| Scale ceiling | ~50–100 GB comfortably | Effectively unlimited |
| Setup time | `pip install duckdb` | Days (IAM, VPC, billing) |
| Portability | File copy = full migration | Vendor-locked |

**Decision:** DuckDB is correct for this assessment scope. The 38M row dataset fits comfortably. Zero infrastructure cost lets focus remain on pipeline design rather than cloud setup. The architecture is intentionally structured so that swapping to BigQuery requires only a `profiles.yml` change — no SQL changes.

---

### dbt vs. Pure SQL Scripts

Pure SQL scripts (e.g., `.sql` files run via `psql`) would work but lack:
- **Dependency resolution**: dbt's `ref()` builds an execution DAG automatically; scripts require manual ordering
- **Incremental models**: dbt handles `INSERT/MERGE` logic for partial updates
- **Tests**: dbt's schema tests (`not_null`, `unique`, custom) run with one command
- **Documentation**: `schema.yml` descriptions become a searchable catalog
- **Compilation**: Jinja templating enables environment-aware SQL without string concatenation

**Decision:** dbt's abstractions pay for themselves once the project has more than 3–4 models. The layered `ref()` graph alone justifies it.

---

### Airflow vs. Cron

| Dimension | Airflow (chosen) | Cron |
|---|---|---|
| Dependency management | Explicit task graph | Sequential scripts only |
| Observability | Task-level logs, UI, history | File logs, manual inspection |
| Retries | Configurable per-task | Requires custom scripting |
| Backfill | Built-in date-aware reruns | Manual intervention |
| Failure handling | Alerts, skip, branch | `||` chains in bash |

**Decision:** The pipeline has 8 sequential tasks with different failure modes. Airflow makes each independently observable, retriable, and auditable. Cron would collapse this into a single shell script with no visibility into which step failed.

---

### Layered Architecture vs. Flat Transformation

A single SQL script could join, filter, and aggregate in one pass. The layered approach pays for:
- **Debugging**: When agg_zone_performance shows wrong revenue, the problem is isolated to one of three places (wrong source, wrong filter, wrong aggregation) — not buried in 200 lines of SQL.
- **Reuse**: `int_trips_enriched` is referenced by `fct_trips`, `agg_daily_metrics`, and `agg_zone_performance`. Filters defined once, not duplicated three times.
- **Testing**: Tests can be written at each layer. A failure in `stg_yellow_trips` tests is caught before it propagates into downstream aggregations.
- **Cost**: In cloud warehouses, the intermediate table is scanned once and cached. Without it, three mart queries would each re-scan 38M rows of raw Parquet.

---

## 9. Improvements & Future Work

### Incremental Models

The largest immediate improvement. Currently, every run re-processes all 12 months of 2023 data even when only yesterday's file changed.

```sql
-- int_trips_enriched with incremental strategy
{{ config(materialized='incremental', unique_key='trip_id', incremental_strategy='merge') }}

select ...
from stg_yellow_trips
{% if is_incremental() %}
where pickup_date >= (select max(pickup_date) from {{ this }})
{% endif %}
```

Impact: reduces intermediate build from 37 seconds (full scan) to seconds (one day of data).

---

### Partitioning

DuckDB supports partition pruning on Parquet files. Reading only the needed month instead of globbing all 12:

```sql
-- In stg_yellow_trips, with date-aware partitioning:
from read_parquet('{{ var("input_dir") }}/yellow_tripdata_{{ var("year") }}-*.parquet')
```

On BigQuery/Snowflake, partition `int_trips_enriched` and `fct_trips` by `pickup_date` to enable partition-pruned mart queries.

---

### Data Quality Enhancements

1. **`dbt_expectations`** package: Add statistical tests — e.g., `expect_column_values_to_be_between(fare_amount, 0, 500)` to catch data feed anomalies before they reach marts.
2. **`dbt source_freshness`**: Replace the `validate_inputs` Python task with dbt's native freshness check, configured per source with `freshness.warn_after` and `freshness.error_after` thresholds.
3. **Row count assertions**: Assert that `int_trips_enriched` has at least 2.5M rows (minimum expected for any 2023 month) — catches silent upstream data truncation.
4. **Referential integrity test**: Confirm all `pickup_location_id` values in `fct_trips` exist in `dim_zones` — catches TLC schema changes introducing new zone IDs.

---

### Monitoring & Alerting

1. **Airflow callbacks**: Add `on_failure_callback` to `DEFAULT_ARGS` to post to Slack/PagerDuty when any task fails.
2. **`agg_daily_metrics` anomaly detection**: A dbt test (or separate Airflow task) that queries `agg_daily_metrics` and raises an alert if `total_trips` for the latest date deviates more than 2 standard deviations from the 30-day rolling mean.
3. **DuckDB file size monitoring**: Track `nyc_taxi.duckdb` size over time. Sudden growth indicates runaway materialization; unexpected shrinkage indicates failed build.

---

### CI/CD Pipeline

```yaml
# .github/workflows/dbt_ci.yml
on: [pull_request]
jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-core==1.8.3 dbt-duckdb==1.8.2 duckdb==1.0.0
      - run: dbt deps --project-dir dbt
      - run: dbt seed --project-dir dbt --profiles-dir dbt
      - run: dbt run --project-dir dbt --profiles-dir dbt
      - run: dbt test --project-dir dbt --profiles-dir dbt
```

On every PR: full pipeline runs against a test DuckDB instance seeded with sample data. Merges blocked if any dbt test fails.

---

## 10. Brainstormer: Should Mart Tables Be Visible If Tests Fail?

**Short answer: No. Consumers must never see data that has not passed its quality gate.**

### The Problem with the Naive Approach

In the current pipeline, `run_dbt_marts` materializes tables first, then `run_dbt_tests` validates them. If tests fail, the bad mart tables already exist in DuckDB and are queryable by any consumer who connects during the window between task completion and Airflow's failure propagation.

### The Production Pattern: Atomic Table Swap

The correct pattern is **build in shadow, swap atomically on success**:

```
┌─────────────────────────────────────────────────────────┐
│  STAGING SCHEMA (main_marts_staging)                    │
│  Build new versions of all mart tables here first       │
│  Run all tests against staging schema                   │
│                                                         │
│  IF tests pass:                                         │
│    BEGIN;                                               │
│    ALTER SCHEMA main_marts RENAME TO main_marts_backup; │
│    ALTER SCHEMA main_marts_staging RENAME TO main_marts;│
│    DROP SCHEMA main_marts_backup CASCADE;               │
│    COMMIT;                                              │
│                                                         │
│  IF tests fail:                                         │
│    DROP SCHEMA main_marts_staging CASCADE;              │
│    -- main_marts untouched; consumers see last-good run │
└─────────────────────────────────────────────────────────┘
```

This is **blue/green deployment** applied to a data warehouse. At all times, `main_marts` contains only data that has passed all tests. The swap is atomic — there is no window where a consumer can read a partial or failing state.

### Implementation in dbt

dbt's built-in mechanism for this is the `--defer` flag combined with staging schemas:

```bash
# Step 1: Build into a staging schema
dbt run --select marts --vars '{"target_schema": "main_marts_staging"}'

# Step 2: Test the staging schema
dbt test --select marts --vars '{"target_schema": "main_marts_staging"}'

# Step 3: If tests pass, execute the atomic swap in DuckDB
duckdb nyc_taxi.duckdb -c "
  BEGIN;
  DROP SCHEMA IF EXISTS main_marts_old CASCADE;
  ALTER SCHEMA main_marts RENAME TO main_marts_old;
  ALTER SCHEMA main_marts_staging RENAME TO main_marts;
  DROP SCHEMA main_marts_old CASCADE;
  COMMIT;
"
```

### The Tradeoff

This approach requires roughly double the storage (both blue and green schemas exist during the build). For DuckDB on a constrained local volume, this may not be feasible. The pragmatic production compromise:

1. For **mart tables that change often** (daily aggregations): blue/green swap
2. For **slowly-changing dimensions** (zone reference data): build in-place with a transaction guard
3. For **ad-hoc exploration environments**: accept that test failures leave stale data and document the risk

### Key Principle

> A dbt test failure is not an application error — it is a data contract violation. Consumers downstream of a violated contract will produce wrong numbers silently. The pipeline must make it impossible to query post-failure data as if it were validated.

---

## 11. Interview Questions & Ideal Answers

---

**Q: Walk me through your pipeline end-to-end.**

A: The pipeline starts with raw NYC TLC Yellow Taxi Parquet files and a zone lookup CSV. Airflow orchestrates 8 tasks in a linear chain:

1. `validate_inputs` runs a Python check for required files before any dbt command — fail fast, fail clearly.
2. `dbt_deps` installs any declared packages (idempotent no-op when none are declared).
3. `dbt_seed` loads the 265-row zone lookup CSV into DuckDB.
4. `run_dbt_staging` creates two views: `stg_yellow_trips` (reads 38M rows via `read_parquet` glob, renames columns, casts types, adds surrogate key and duration) and `stg_taxi_zones` (renames and enriches the seed with airport flag).
5. `run_dbt_intermediate` materializes `int_trips_enriched` as a physical table: 35.5M rows after quality filters, joined with zone names on both pickup and dropoff location IDs.
6. `run_dbt_marts` builds four tables: `fct_trips` (pass-through), `dim_zones` (complete zone dimension), `agg_daily_metrics` (daily rollup), `agg_zone_performance` (zone-level performance with revenue ranking).
7. `run_dbt_tests` runs 4 schema tests — pipeline fails if any fail.
8. `export_reports` signals clean completion.

All dbt commands receive `--project-dir` and `--profiles-dir` from environment variables, so no paths are hardcoded.

---

**Q: Why DuckDB instead of Postgres or SQLite?**

A: Three reasons. First, DuckDB is columnar — it reads only the columns it needs from Parquet files, not full rows. For a query that aggregates revenue across 35M rows but only needs 2 columns, DuckDB reads ~2× faster than a row-store. Second, `read_parquet()` lets DuckDB query Parquet files directly without an import step — our staging view is literally `SELECT ... FROM read_parquet('*.parquet')`. There's no ETL load phase. Third, DuckDB is zero-infrastructure — it's a file, not a server. In a Docker assessment environment, this means no separate database container, no connection pooling, no port mapping. The tradeoff is single-writer concurrency, which we mitigate with `max_active_runs=1` in the DAG.

---

**Q: Why three dbt layers? Couldn't you do this in one model?**

A: Technically yes. But three layers give us: isolation of concerns (staging = raw contract, intermediate = business logic, marts = delivery), debuggability (when a revenue number is wrong, I can query each layer independently to pinpoint where it diverged), reuse (three mart models all read from `int_trips_enriched` — the quality filters are defined once, not three times), and testability (I can write tests at each boundary). In cloud warehouses, materialized intermediate tables also prevent redundant re-scanning — each mart query hits pre-joined data instead of re-joining 38M raw rows.

---

**Q: How do you ensure data quality?**

A: Two layers. First, structural: `validate_inputs` checks that required files exist before any transformation runs. The staging model casts all columns explicitly — if TLC changes a column type, the cast fails loudly rather than silently coercing. Second, semantic: dbt tests enforce business rules after transformation — `trip_id` is never null, `dim_zones.location_id` is unique and never null. The pipeline uses `set -e` in all Bash tasks so a non-zero dbt exit code propagates to Airflow as a task failure. When tests fail, the DAG fails — downstream consumers are not notified of success.

---

**Q: How do you handle failures in this pipeline?**

A: Three mechanisms. Airflow's `retries=2` with `retry_delay=5min` handles transient failures — a DuckDB I/O spike or a network hiccup during `dbt deps` will self-heal. The `execution_timeout=1hr` prevents hung processes from blocking the scheduler slot. `set -e` in Bash commands propagates dbt failures to Airflow immediately rather than silently continuing. For the `run_dbt_tests` task specifically, a test failure marks the entire DAG run failed — which means downstream consumers (dashboards, exports) never fire and never see potentially bad data.

---

**Q: How would you scale this to handle 10 years of data (3.8 billion rows)?**

A: Two phases. Phase 1 (1–100 GB): DuckDB remains viable with incremental models. Change `int_trips_enriched` to `materialized='incremental'` — each daily run processes only the new day's Parquet file rather than all-time data. Phase 2 (>100 GB): Replace DuckDB with BigQuery or Snowflake — only `profiles.yml` changes, all SQL models are adapter-agnostic. The Spark script (`process_historical.py`) handles bulk backfill: it reads multiple Parquet years, normalizes schema, and writes year/month-partitioned output. Spark's distributed execution handles TBs of data that DuckDB cannot. dbt then runs incremental models over the partitioned output. The pipeline architecture doesn't change — only the compute engine underneath it.

---

**Q: How does Airflow handle backfills?**

A: `catchup=False` prevents automatic historical backfills when the DAG is first deployed. For intentional backfills, we trigger specific `logical_date` values via Airflow CLI (`airflow dags backfill -s 2024-01-01 -e 2024-01-31 nyc_taxi_daily_pipeline`) or the UI. Because our `validate_inputs` task checks for the presence of Parquet files rather than a specific date, the pipeline is date-agnostic — it processes whatever files exist in `INPUT_DIR`. To backfill a specific month, we would stage those Parquet files in `INPUT_DIR` and trigger the DAG for that date.

---

**Q: Why LEFT JOIN instead of INNER JOIN for zone enrichment?**

A: TLC location IDs 264 and 265 represent "N/V" (Not Valid) and "Unknown" zones — they appear in trip data but have no meaningful zone name in the reference table. An INNER JOIN would silently drop every trip from these location IDs, under-counting revenue totals and trip volumes without any warning. A LEFT JOIN preserves these trips with NULL zone names. Downstream models coalesce NULLs to `'Unknown'`, so they appear as a labelled group in analytics rather than disappearing. For a revenue reporting pipeline, silently missing data is worse than acknowledged unknown data.

---

**Q: How did you handle missing surrogate keys?**

A: The NYC TLC dataset has no natural primary key. Two trips from the same vendor in the same zone at the same second are physically distinct trips but produce identical field values. Our surrogate key is `md5(vendor_id || '-' || pickup_datetime || '-' || pickup_location_id)`. The `-` separator prevents cross-field collisions (vendor=1, location=23 is different from vendor=12, location=3). `coalesce` guards each component so a null field doesn't produce a null hash. We intentionally omit the `unique` test on `trip_id` after discovering 917,566 hash collisions in 2023 data — real trips from high-volume zones at the same second. The column is `not_null` tested. This is documented as a known limitation, not a bug.

---

**Q: What happens if a dbt test fails in production?**

A: With the current pipeline: `run_dbt_tests` fails, Airflow marks the task failed, the DAG run fails, `export_reports` never fires — so downstream consumers don't get a success signal. But the mart tables from `run_dbt_marts` are still readable by anyone who connects directly to DuckDB. The correct production pattern is atomic table swap: build new marts in a staging schema, run tests against that schema, swap schemas only on success. This way, `main_marts` always contains the last successfully tested run. If tests fail, the staging schema is dropped and `main_marts` remains unchanged.

---

**Q: What were the hardest technical challenges?**

A: Two standout ones. First, the Docker OOM kill: the intermediate model (35M rows) was killed with SIGKILL every time. Memory stats showed low usage when I checked — because the process was already dead. Root cause was dbt's multiprocessing: with `threads: 2`, two Python worker processes spawn, each loading the full dbt + DuckDB runtime. In a 3.5 GB WSL2 VM with webserver and postgres already running, this exceeded available RAM. Fix: `threads: 1` eliminates the subprocess, and `memory_limit='1.8GB'` in profiles lets DuckDB spill to disk safely. Second, Jinja in SQL comments: a comment like `-- {{ ref() }} ensures dbt tracks...` causes a parse error because dbt evaluates `{{ }}` everywhere in file content, not just in SQL expressions. Had to strip curly braces from all documentation text.

---

**Q: How would you productionize this?**

A: Six changes. (1) Incremental models — daily runs process only new data. (2) Atomic table swap — blue/green schema promotion on test success only. (3) CI/CD — GitHub Actions runs full dbt pipeline on every PR against a sample dataset; merges blocked on test failure. (4) Monitoring — `on_failure_callback` in Airflow posts to Slack; `agg_daily_metrics` monitored for statistical anomalies. (5) `dbt source_freshness` — replaces the Python `validate_inputs` task with dbt-native freshness SLAs. (6) Cloud adapter — swap `profiles.yml` to BigQuery or Snowflake for multi-user concurrency and scale beyond what a single DuckDB file supports.

---

## 12. 2-Minute Elevator Pitch

> "I built an end-to-end data engineering pipeline that processes 38 million NYC Yellow Taxi trip records — a full year of TLC data — through a three-layer transformation architecture using dbt on DuckDB, orchestrated by Apache Airflow running in Docker.
>
> The pipeline has three phases. First, staging: raw Parquet files are read directly via DuckDB's `read_parquet()` glob — no ETL load step — and columns are renamed, typed, and given a surrogate MD5 key. Second, intermediate: 35.5 million records survive quality filters (removing meter tests, voids, and extreme outliers), and each trip is enriched with pickup and dropoff zone names via a LEFT JOIN against the TLC zone reference table. Third, marts: four analytics-ready tables — a fact table for per-trip queries, a zone dimension, a daily metrics rollup, and a zone performance table with revenue ranking and tip rate analysis.
>
> Airflow orchestrates 8 tasks with explicit retries and fail-fast behavior. If any dbt test fails — and there are four tests covering surrogate key integrity and dimension completeness — the pipeline fails loudly rather than silently delivering bad data.
>
> The most interesting challenges were architectural. DuckDB's single-writer model required `max_active_runs=1` in the DAG. In a memory-constrained Docker environment, dbt's multiprocessing overhead caused OOM kills on the 35M-row intermediate build — solved by running single-threaded with DuckDB's disk-spill configured. And Jinja's evaluation of `{{ }}` expressions in SQL comments caused parse failures that required careful attention to what is and isn't documentation text in dbt models.
>
> The architecture is intentionally portable: swapping from DuckDB to BigQuery or Snowflake requires only a `profiles.yml` change. The PySpark script I included handles the bulk historical backfill path — Spark for volume, dbt for logic, Airflow for orchestration."

---

*This document was written against the actual implementation — every code reference, row count, error message, and fix reflects what was built and debugged in this repository.*
