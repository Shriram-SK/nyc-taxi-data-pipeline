# NYC Yellow Taxi — Data Engineering Pipeline

> **End-to-end batch pipeline** processing 38.3 million NYC TLC trip records through a three-layer dbt transformation on DuckDB, orchestrated daily by Apache Airflow running in Docker.

---

## Elevator Pitch

This pipeline ingests all 12 months of NYC Yellow Taxi 2023 Parquet data, cleans and enriches it through a layered dbt model hierarchy, and delivers four analytics-ready tables — a trip fact table, zone dimension, daily metrics rollup, and zone performance ranking — tested for data quality on every run and orchestrated by Airflow with per-task retry granularity.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset](#2-dataset)
3. [Architecture](#3-architecture)
4. [Tech Stack](#4-tech-stack)
5. [Data Modeling](#5-data-modeling)
6. [Key Transformations](#6-key-transformations)
7. [Airflow DAG](#7-airflow-dag)
8. [Data Quality](#8-data-quality)
9. [SQL Analysis](#9-sql-analysis)
10. [Scalability](#10-scalability)
11. [Design Decisions](#11-design-decisions)
12. [Challenges & Solutions](#12-challenges--solutions)
13. [Brainstormer: Blue-Green Deployment](#13-brainstormer-blue-green-deployment)
14. [Setup & Running](#14-setup--running)
15. [Project Structure](#15-project-structure)
16. [Future Improvements](#16-future-improvements)

---

## 1. Project Overview

The NYC TLC publishes raw Yellow Taxi trip records monthly as Parquet files. In raw form they are analytically unusable: column names are TLC-specific PascalCase, types are inconsistent across months, there is no primary key, invalid records (meter tests, voids, zero-distance trips) are interleaved with real data, and pickup/dropoff locations exist only as opaque integer IDs.

This pipeline transforms that raw data into a clean, tested, analytics-ready data mart that answers:

- Which pickup zones generate the most revenue?
- What time of day do trip volumes peak and trough?
- How long does a given zone go unserved between consecutive pickups?

**Outputs:**

| Table | Grain | Description |
|---|---|---|
| `fct_trips` | One row per trip | Core fact table — 35.5M quality-filtered, zone-enriched records |
| `dim_zones` | One row per TLC location ID | Complete zone dimension — all 265 locations regardless of trip coverage |
| `agg_daily_metrics` | One row per date | Daily volume, revenue, fare, distance, duration, and tip rollups |
| `agg_zone_performance` | One row per pickup zone | Revenue rank, tip rate, average speed, high-volume zone flag |

---

## 2. Dataset

| Source | Format | Scale |
|---|---|---|
| NYC TLC Yellow Taxi 2023 | Parquet (monthly files) | 38,310,226 raw rows across 12 files |
| TLC Taxi Zone Lookup | CSV | 265 rows — zone name, borough, service classification |

**Download:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Key raw columns: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `fare_amount`, `tip_amount`, `total_amount`, `payment_type`.

After quality filtering: **35,468,135 rows** survive (~7.4% removed as structurally invalid).

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  RAW INPUT  (/opt/airflow/data/input)                               │
│                                                                     │
│  yellow_tripdata_2023-01.parquet  ──┐                               │
│  yellow_tripdata_2023-02.parquet    │  38.3M rows total             │
│  ...                                │                               │
│  yellow_tripdata_2023-12.parquet  ──┤                               │
│  taxi_zone_lookup.csv ──────────────┘                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
            ┌───────────────▼────────────────┐
            │  Apache Airflow                │
            │  DAG: nyc_taxi_daily_pipeline  │
            │  Schedule: 0 2 * * * (UTC)     │
            │                                │
            │  check_source_freshness        │
            │          ↓                     │
            │      dbt_deps                  │
            │          ↓                     │
            │      dbt_seed ──────────────── │──► loads taxi_zone_lookup
            │          ↓                     │
            │   run_dbt_staging              │
            │          ↓                     │
            │  run_dbt_intermediate          │
            │          ↓                     │
            │    run_dbt_marts               │
            │          ↓                     │
            │    run_dbt_tests               │
            │          ↓                     │
            │    notify_success              │
            └───────────┬────────────────────┘
                        │
            ┌───────────▼────────────────────────────────┐
            │  DuckDB  (nyc_taxi.duckdb)                 │
            │  Catalog: nyc_taxi                         │
            │                                            │
            │  main_seeds                                │
            │    └─ taxi_zone_lookup          (265 rows) │
            │                                            │
            │  main_staging             (views)          │
            │    ├─ stg_yellow_trips  ← read_parquet()   │
            │    └─ stg_taxi_zones   ← ref(seed)         │
            │                                            │
            │  main_intermediate        (table)          │
            │    └─ int_trips_enriched  35.5M rows       │
            │                                            │
            │  main_marts               (tables)         │
            │    ├─ fct_trips                            │
            │    ├─ dim_zones                            │
            │    ├─ agg_daily_metrics                    │
            │    └─ agg_zone_performance                 │
            └────────────────────────────────────────────┘
                        │
            ┌───────────▼──────────────────┐
            │  Ad-hoc SQL  (queries/)      │
            │  q1  Top zones by revenue    │
            │  q2  Hour-of-day patterns    │
            │  q3  Inter-trip gap analysis │
            └──────────────────────────────┘
```

**dbt Lineage:**

```
taxi_zone_lookup.csv
    └── [seed] taxi_zone_lookup
            └── stg_taxi_zones (view)
                    ├── int_trips_enriched (table) ←── stg_yellow_trips (view)
                    │       ├── fct_trips (table)             ↑
                    │       ├── agg_daily_metrics (table)     │
                    │       └── agg_zone_performance (table)  │
                    └── dim_zones (table)          yellow_tripdata_*.parquet
```

---

## 4. Tech Stack

| Tool | Version | Role |
|---|---|---|
| **DuckDB** | 1.0.0 | In-process columnar OLAP engine. Reads Parquet natively via `read_parquet()`. Zero infrastructure — a single `.duckdb` file. |
| **dbt-core** | 1.8.3 | Transformation framework. Provides dependency resolution, incremental models, schema tests, and documentation from a single SQL + YAML project. |
| **dbt-duckdb** | 1.8.2 | DuckDB adapter for dbt. Handles catalog naming, schema creation, and the `read_parquet()` source pattern. |
| **Apache Airflow** | 2.9.1 | DAG-based orchestration with per-task retry, UI, and execution history. Runs in Docker. |
| **Docker Compose** | — | Reproducible Airflow stack: scheduler + webserver + Postgres metadata DB, with named volumes for DuckDB persistence. |
| **PySpark** | 3.5.1 | Optional: schema normalisation and Parquet partitioning for multi-year historical backfills that exceed single-node DuckDB capacity. |
| **Python** | 3.11 | Input validation, Airflow DAG logic, Spark job. |

---

## 5. Data Modeling

The pipeline follows dbt's canonical three-layer architecture. Each layer has a single, bounded responsibility. Business logic lives in exactly one place.

### Layer 1 — Staging (`main_staging`) · Materialized as **views**

Views cost nothing to rebuild and always reflect the current Parquet/seed state. No business logic, no filtering — staging is a typed contract with the source.

**`stg_yellow_trips`**

Reads all 12 monthly Parquet files in a single glob:
```sql
FROM read_parquet('{{ var("input_dir") }}/yellow_tripdata_2023-*.parquet')
```
Responsibilities: rename PascalCase TLC columns to `snake_case`, cast every column explicitly, generate `trip_id` surrogate key, derive `pickup_date` and `trip_duration_minutes`.

**`stg_taxi_zones`**

Sources from the dbt seed `taxi_zone_lookup`. Adds `zone_clean` (lowercased for case-insensitive joins) and `is_airport` boolean (`service_zone = 'Airports'`). All 265 zones retained — no filtering — so downstream LEFT JOINs have complete coverage.

---

### Layer 2 — Intermediate (`main_intermediate`) · Materialized as a **table**

All joins, quality filters, and derived columns live here and nowhere else. Materializing as a table means three mart models query pre-built data instead of re-scanning 38M Parquet rows on every run.

**`int_trips_enriched`** (35,468,135 rows)

```
stg_yellow_trips (38.3M)
    LEFT JOIN stg_taxi_zones  ON pickup_location_id  = location_id  → pickup_zone_name, pickup_borough
    LEFT JOIN stg_taxi_zones  ON dropoff_location_id = location_id  → dropoff_zone_name, dropoff_borough
    WHERE trip_distance > 0
      AND fare_amount > 0
      AND passenger_count > 0
      AND trip_duration_minutes BETWEEN 1 AND 180
```

Added columns: `revenue` (coalesced `total_amount`), `trip_month` (date truncated), `is_long_trip` (> 60 min), `trip_speed_mph` (distance / duration).

---

### Layer 3 — Marts (`main_marts`) · Materialized as **tables**

Thin delivery layer. No new transformation — aggregate or pass through the intermediate table.

| Model | Grain | Key design note |
|---|---|---|
| `fct_trips` | 1 row / trip | Direct pass-through. Stable contract for BI tools and ad-hoc analysts. |
| `dim_zones` | 1 row / location | Sourced from `stg_taxi_zones` (not intermediate) so all 265 zones are present even if a zone had zero trips. |
| `agg_daily_metrics` | 1 row / date | Uses `avg(fare_amount)` not `avg(total_amount)` — isolates base fare trend from tip/surcharge volatility. |
| `agg_zone_performance` | 1 row / pickup zone | `RANK()` (not `ROW_NUMBER()`) for tie-safe revenue ranking. `tip_rate_pct = sum(tip) / nullif(sum(revenue), 0) * 100`. `is_high_volume_zone` flag at 100k trips/year threshold. |

---

## 6. Key Transformations

### Surrogate Key

No natural primary key exists in TLC data. Two trips from the same vendor in the same zone at the same second are physically distinct but have identical field values.

```sql
md5(
    coalesce(cast(VendorID as varchar), '')           || '-' ||
    coalesce(cast(tpep_pickup_datetime as varchar), '') || '-' ||
    coalesce(cast(PULocationID as varchar), '')
) as trip_id
```

The `'-'` separator prevents cross-field hash collisions (vendor=`1`, location=`23` must differ from vendor=`12`, location=`3`). `coalesce(..., '')` ensures a null component produces an empty string, not a null hash.

> **Known limitation:** 917,566 hash collisions exist in the 2023 dataset — same vendor, same second, same zone. These are real trips, not errors. The `unique` test is intentionally omitted; `not_null` is tested instead.

### Null-Safe Duration

```sql
case
    when tpep_pickup_datetime is null
      or tpep_dropoff_datetime is null
      or tpep_dropoff_datetime < tpep_pickup_datetime
    then null
    else round(
        (epoch(cast(tpep_dropoff_datetime as timestamp))
       - epoch(cast(tpep_pickup_datetime as timestamp))) / 60.0,
        2
    )
end as trip_duration_minutes
```

Epoch arithmetic preserves sub-minute precision. The `dropoff < pickup` guard handles timestamp inversions in raw TLC data without raising an error. NULL durations then fail the `BETWEEN 1 AND 180` filter in the intermediate layer implicitly — no separate `IS NOT NULL` needed.

### LEFT JOIN for Zone Enrichment

TLC location IDs 264 ("NV") and 265 ("Unknown") appear in trip data but have no zone record. An `INNER JOIN` would silently drop these trips and under-count revenue. A `LEFT JOIN` preserves them with `NULL` zone names; downstream models `coalesce(pickup_zone_name, 'Unknown')` to keep them visible in reports.

### Quality Filters (intermediate layer only)

| Filter | Rationale |
|---|---|
| `trip_distance > 0` | Zero-distance = meter test or cancellation |
| `fare_amount > 0` | Non-positive fare = void, dispute, or no-charge code |
| `passenger_count > 0` | Driver-entered; 0 means field was not set |
| `trip_duration_minutes BETWEEN 1 AND 180` | < 1 min = meter error; > 3 hrs = extreme outlier skewing averages |

Filters live in the intermediate layer, not staging, so staging remains a faithful record of what the source contained. Auditing what was dropped is always possible by querying staging vs. intermediate.

---

## 7. Airflow DAG

**DAG ID:** `nyc_taxi_daily_pipeline`
**Schedule:** `0 2 * * *` (02:00 UTC daily)
**Catchup:** enabled — supports date-aware backfills
**Max active runs:** 1 — DuckDB is single-writer; concurrent runs would deadlock

```
check_source_freshness
        ↓
    dbt_deps
        ↓
    dbt_seed
        ↓
 run_dbt_staging
        ↓
run_dbt_intermediate
        ↓
  run_dbt_marts
        ↓
  run_dbt_tests
        ↓
  notify_success
```

### Task Reference

| Task | Operator | Purpose |
|---|---|---|
| `check_source_freshness` | PythonOperator | Uses Airflow `execution_date` (via `context["ds"]`) to check for Parquet files matching the prior month's pattern. Fails fast before any dbt compute if data hasn't arrived. Supports backfill — each triggered date checks its own expected files. |
| `dbt_deps` | BashOperator | Resolves `packages.yml` on every run so package updates take effect without image rebuilds. No-op when no packages are declared. |
| `dbt_seed` | BashOperator | Loads `taxi_zone_lookup.csv` → `main_seeds.taxi_zone_lookup`. **Must precede staging** — `stg_taxi_zones` uses `{{ ref('taxi_zone_lookup') }}`. |
| `run_dbt_staging` | BashOperator | `dbt run --select staging` — creates/replaces the two staging views. |
| `run_dbt_intermediate` | BashOperator | `dbt run --select intermediate` — materializes `int_trips_enriched` as a physical table. Most resource-intensive step. |
| `run_dbt_marts` | BashOperator | `dbt run --select marts` — builds all four mart tables from the already-materialized intermediate table. |
| `run_dbt_tests` | BashOperator | `dbt test` — runs 4 schema tests. **Pipeline fails here if any test fails.** Mart tables exist but downstream consumers are blocked from a success signal. |
| `notify_success` | BashOperator | Queries `fct_trips` for final trip count and total revenue. Logs the result as a run summary. Extensible to Slack/PagerDuty. |

All BashOperator commands use `set -e` — the shell exits immediately on any non-zero dbt return code, propagating failures to Airflow as task failures rather than warnings.

### Reliability Configuration

```python
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": True,
}
```

Two retries with 5-minute delay handles transient I/O spikes or `dbt deps` network blips. The 1-hour timeout prevents hung processes from blocking the scheduler slot indefinitely.

---

## 8. Data Quality

### dbt Schema Tests

```yaml
# staging/schema.yml
- name: stg_yellow_trips
  columns:
    - name: trip_id
      data_tests: [not_null]          # unique omitted — 917,566 collisions in 2023 data (real trips)

# marts/schema.yml
- name: dim_zones
  columns:
    - name: location_id
      data_tests: [unique, not_null]  # primary key integrity

- name: fct_trips
  columns:
    - name: trip_id
      data_tests: [not_null]
```

### Quality Gates

| Gate | Mechanism | On failure |
|---|---|---|
| Source files present | `check_source_freshness` Python task | DAG fails before any transformation runs |
| Column-level integrity | dbt `data_tests` in `schema.yml` | `run_dbt_tests` exits non-zero → task fails → DAG fails |
| Structural invalidity | Explicit casts in staging models | Type mismatch raises a DuckDB error → model fails to build |
| Business validity | Quality filters in `int_trips_enriched` | Invalid rows filtered out; count difference visible in dbt run logs |

Tests use `data_tests:` (not the deprecated `tests:` key) — required for dbt 1.8 compatibility.

---

## 9. SQL Analysis

Three standalone queries in `queries/` run directly against the DuckDB file and demonstrate advanced SQL patterns against `intermediate.int_trips_enriched`.

### Q1 — Top Zones by Revenue (`q1_top_zones_by_revenue.sql`)

**Question:** Which 10 pickup zones generated the most revenue in 2023?

```sql
select
    coalesce(pickup_zone_name, 'Unknown') as pickup_zone_name,
    round(sum(revenue), 2)               as total_revenue
from intermediate.int_trips_enriched
group by coalesce(pickup_zone_name, 'Unknown')
order by total_revenue desc, pickup_zone_name
limit 10;
```

`coalesce` ensures TLC location IDs 264/265 appear as a labelled group rather than being excluded by NULL semantics. The secondary `order by pickup_zone_name` makes output deterministic when revenue values tie.

---

### Q2 — Hour-of-Day Trip Pattern (`q2_hour_of_day_pattern.sql`)

**Question:** How many trips occur in each hour of the day, guaranteed for all 24 hours?

```sql
with hours as (
    select * from generate_series(0, 23) as hour_of_day
),
trips as (
    select cast(extract(hour from pickup_datetime) as integer) as hour_of_day,
           count(*) as total_trips
    from intermediate.int_trips_enriched
    group by hour_of_day
)
select h.hour_of_day, coalesce(t.total_trips, 0) as total_trips
from hours h
left join trips t on h.hour_of_day = t.hour_of_day
order by h.hour_of_day;
```

`generate_series(0, 23)` creates a **complete spine** of all 24 hours. Without it, hours with zero trips are absent from results — silent gaps that mislead time-series charts. The LEFT JOIN + `coalesce` converts absence into an explicit zero.

---

### Q3 — Consecutive Gap Analysis (`q3_consecutive_gap_analysis.sql`)

**Question:** Within each pickup zone, how long does the zone go unserved between consecutive trips?

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
    pickup_zone_name, pickup_datetime, previous_trip_time,
    case
        when previous_trip_time is not null
         and pickup_datetime >= previous_trip_time
        then round((epoch(pickup_datetime) - epoch(previous_trip_time)) / 60.0, 2)
    end as gap_minutes
from ordered
where previous_trip_time is not null
order by pickup_zone_name, pickup_datetime;
```

Partitioned by **zone** (not by vendor/driver) — answers "how long is this zone unserved?" which drives driver positioning decisions. Epoch subtraction preserves sub-minute precision. The `CASE WHEN pickup_datetime >= previous_trip_time` guard handles rare timestamp inversions in raw TLC data; without it, negative gaps would corrupt rolling-average aggregations.

---

## 10. Scalability

### Current Ceiling

DuckDB is single-writer and in-process. It handles the 38M-row 2023 dataset comfortably. Beyond ~50–100 GB, or when concurrent writers are needed, it becomes a bottleneck.

### PySpark Backfill (`spark/process_historical.py`)

For multi-year or >100 GB loads, the included Spark job preprocesses raw TLC files before handing off to dbt:

```bash
spark-submit spark/process_historical.py \
  --input_dir /data/raw/yellow_taxi \
  --output_dir /data/processed/yellow_taxi \
  --year 2023
```

It: normalises column names (same renames as dbt staging), validates structural integrity (null timestamps, null location IDs), and writes output partitioned by `year` / `month` — compatible with the staging glob pattern. Business logic stays in dbt; Spark handles only volume.

### Scaling Path

| Dataset size | Architecture |
|---|---|
| < 50 GB | DuckDB + dbt (current setup) |
| 50 GB – 5 TB | Swap `profiles.yml` to dbt-bigquery or dbt-snowflake. All SQL models are adapter-agnostic — no rewrite needed. |
| > 5 TB / streaming | Spark Structured Streaming → Delta Lake / Iceberg → dbt incremental models with `merge` strategy. |

**Key lever — incremental models:** Changing `int_trips_enriched` from `materialized='table'` to `materialized='incremental'` with `unique_key='trip_id'` reduces daily runtime from a full 35M-row rebuild to processing only the new day's file.

---

## 11. Design Decisions

### Why DuckDB instead of Postgres or a cloud warehouse?

DuckDB reads Parquet files in-place via `read_parquet()` — there is no load step, no import, no ETL. Columnar vectorized execution delivers sub-second aggregations over 35M rows with no cluster to provision, no cloud account, and no network cost. The `dbt-duckdb` adapter is interface-compatible with `dbt-bigquery` and `dbt-snowflake` — migrating is a `profiles.yml` change, not a code change.

The `quoting: false` setting in `dbt_project.yml` is a DuckDB-specific requirement: DuckDB stores unquoted identifiers in lowercase. Enabling quoting causes dbt to emit double-quoted identifiers that DuckDB treats as case-sensitive, producing `relation not found` errors.

### Why dbt instead of raw SQL scripts?

Raw SQL scripts would work but lack: automatic dependency resolution (`ref()` builds an execution DAG — no manual ordering), incremental model support (INSERT/MERGE logic handled by the framework), schema tests that run with one command, compiled lineage documentation, and Jinja templating for environment-aware SQL. The overhead pays for itself once a project has more than three models.

### Why Airflow instead of cron?

The pipeline has 8 sequential tasks with different failure modes, resource requirements, and retry policies. Airflow makes each independently observable, retriable, and auditable. A cron job collapses this into a single shell script with no visibility into which step failed and no built-in backfill support. `execution_date`-aware tasks (`check_source_freshness`) would require significant custom scripting in bash.

### Why local files instead of S3?

For an assessment scope with a defined 2023 dataset, local Parquet files eliminate cloud credential management, network latency, and cost. The architecture is S3-ready: the `input_dir` dbt variable and the `INPUT_DIR` Airflow environment variable can be pointed at an S3 path (with the appropriate fsspec/s3fs setup) without changing any SQL. The staging model's `read_parquet()` glob works identically against `s3://bucket/prefix/*.parquet`.

---

## 12. Challenges & Solutions

### dbt CLI hanging with no output

**Root cause:** The installed package was the legacy monolithic `dbt==1.0.0.40.15` — a pre-adapter-split package that predates `dbt-core`. It accepted commands without error but produced no output and never terminated.

**Fix:** Uninstall the legacy package; install the adapter-specific split:
```bash
pip uninstall dbt
pip install dbt-core==1.8.3 dbt-duckdb==1.8.2 duckdb==1.0.0
```
Verify with `dbt --version` — the output should show both Core and plugin versions.

---

### `profiles.yml` contained Jinja — evaluated at parse time

**Root cause:** `profiles.yml` shipped with `path: "{{ env_var('DUCKDB_PATH') }}"`. The env var was not set in the local shell. dbt evaluates `profiles.yml` with Jinja at startup — if the env var is absent, the profile fails to parse before any connection is attempted.

**Fix:** Replaced with a hardcoded absolute path for local development. The Docker version uses the container path directly (`/opt/airflow/data/db/nyc_taxi.duckdb`). The `.example` file retains the `env_var()` pattern as documentation.

---

### `{{ ref() }}` in SQL comments caused parse errors

**Root cause:** A comment in `stg_taxi_zones.sql` read `-- {{ ref() }} ensures dbt tracks...`. dbt's Jinja engine processes `{{ }}` expressions in ALL file content — block comments, line comments, and YAML comments are not safe from evaluation. `{{ ref() }}` with no arguments raised a compilation error.

**Fix:** Stripped `{{ }}` delimiters from all documentation text. References to dbt functions in comments use plain text: `-- ref() ensures dbt tracks...`

---

### `+database: main` caused "Catalog does not exist"

**Root cause:** `dbt_project.yml` had `+database: main` in the model config. In dbt-duckdb, the catalog name is derived from the DuckDB filename — `nyc_taxi.duckdb` creates catalog `nyc_taxi`. Hardcoding `main` overrides this with a catalog that doesn't exist.

**Fix:** Removed `+database: main` entirely. Added a comment in `dbt_project.yml` explicitly documenting why this setting must not be used with dbt-duckdb.

---

### Windows absolute path broke Docker execution

**Root cause:** `dbt_project.yml` had `input_dir: "C:/Users/.../input"` hardcoded as the default. This path is valid on the Windows host but meaningless inside the Linux Docker container.

**Fix:** Changed the default to the container path (`/opt/airflow/data/input`). Local development overrides with `--vars '{"input_dir": "C:/path/to/input"}'` or an environment variable. The path is never hardcoded in committed code.

---

### DuckDB write permission denied on Docker named volume

**Root cause:** The Docker named volume `airflow-data` mounted at `/opt/airflow/data/db` was initialised as `root:root` with permissions `755`. The Airflow process runs as `uid=50000` — not root — and could not write to the directory.

**Fix:** Added a `mkdir -p` + `chmod 777` to the `airflow-init` service in `docker-compose.yml`. This runs once before the scheduler and webserver start, and persists for the lifetime of the volume:
```yaml
command:
  - -c
  - |
    mkdir -p /opt/airflow/data/db && chmod 777 /opt/airflow/data/db
    airflow db migrate
    ...
```

---

### OOM kill during intermediate materialization

**Root cause:** Docker Desktop's WSL2 VM had 3.5 GB total RAM shared across all containers. dbt 1.8 spawns multiprocessing worker processes — with `threads: 2`, two Python workers launched, each loading the full dbt + DuckDB runtime. Combined with the running webserver and postgres, this exceeded available RAM. The process was killed with SIGKILL (exit 137) before any data was written.

**Investigation:** Direct Python `duckdb.connect()` + `CREATE TABLE AS SELECT` completed successfully — confirming the issue was dbt's subprocess overhead, not DuckDB's SQL execution.

**Fix:**
```yaml
# profiles.yml
dev:
  threads: 1               # eliminates subprocess; executes in the main process
  settings:
    memory_limit: "1.8GB"  # DuckDB spills to disk rather than being killed
    temp_directory: "/opt/airflow/data/db/duckdb_tmp"
```
With `threads: 1`, the intermediate model materializes in ~37 seconds.

---

### Schema name confusion — `main_intermediate` vs `intermediate`

**Root cause:** dbt-duckdb prepends the target schema to custom schemas. With `schema: main` in `profiles.yml` and `+schema: intermediate` in `dbt_project.yml`, the physical schema becomes `main_intermediate`. Ad-hoc queries written with `FROM intermediate.int_trips_enriched` fail because the schema doesn't exist.

**Fix:** All standalone SQL queries in `queries/` use `intermediate.int_trips_enriched` with a comment noting that the physical schema in DuckDB is `main_intermediate`. A future improvement is a `generate_schema_name` macro that produces clean schema names without the prefix.

---

## 13. Brainstormer: Blue-Green Deployment

**Question:** If dbt tests fail, should mart tables be visible to consumers?

**Answer: No — never.**

### The Problem

In the current execution order, `run_dbt_marts` builds tables before `run_dbt_tests` validates them. If tests fail, the new mart tables already exist in DuckDB and are queryable by any consumer who connects between task completion and the DAG's failure propagation. A dashboard refreshing on a 5-minute schedule would serve bad data with no indication it failed validation.

### The Solution: Atomic Schema Swap

The correct pattern is **build in a shadow schema, swap atomically only on test success**:

```
Step 1: Build marts in a staging schema
    dbt run --select marts --vars '{"target_schema": "main_marts_staging"}'

Step 2: Run all tests against the staging schema
    dbt test --select marts --vars '{"target_schema": "main_marts_staging"}'

Step 3a: Tests PASS → atomic swap (no window where consumers see bad data)
    BEGIN;
    DROP SCHEMA IF EXISTS main_marts_old CASCADE;
    ALTER SCHEMA main_marts RENAME TO main_marts_old;
    ALTER SCHEMA main_marts_staging RENAME TO main_marts;
    DROP SCHEMA main_marts_old CASCADE;
    COMMIT;

Step 3b: Tests FAIL → discard shadow schema (main_marts untouched)
    DROP SCHEMA main_marts_staging CASCADE;
    -- consumers continue reading last-good validated run
```

This is **blue/green deployment** applied to a data warehouse. At every point in time, `main_marts` contains only data that has passed all quality tests. The schema rename is atomic — there is no window where a consumer can observe a partial or failing state.

### Why This Matters

A dbt test failure is not a software error — it is a data contract violation. Downstream consumers of a violated contract produce wrong numbers silently and confidently. The pipeline must make it architecturally impossible to query post-failure data as if it were validated.

**Practical tradeoff:** This approach requires ~2× storage during the build window (both blue and green schemas coexist). For the Docker development environment with constrained disk, the pragmatic compromise is to accept in-place builds — but document the risk explicitly, and implement the swap pattern before any production deployment.

---

## 14. Setup & Running

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11+ | For local dbt runs |
| Docker Desktop | 24+ | For Airflow stack |
| Java | 11 or 17 | Only for PySpark backfill |
| Docker memory | ≥ 6 GB recommended | WSL2 VMs with 3.5 GB require `threads: 1` in profiles |

### Local dbt (without Docker)

**1. Clone and install**
```bash
git clone <repo-url>
cd nyc-taxi-pipeline
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**2. Configure profiles**
```bash
# Create ~/.dbt/profiles.yml with local DuckDB path
cat > ~/.dbt/profiles.yml << 'EOF'
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "/absolute/path/to/nyc_taxi.duckdb"
      threads: 4
      schema: main
EOF
```

**3. Download source data** into `input/`
```
input/
  yellow_tripdata_2023-01.parquet
  ...
  yellow_tripdata_2023-12.parquet
  taxi_zone_lookup.csv
```

**4. Run the full pipeline**
```bash
cd dbt

dbt deps                                   # install packages (no-op if packages.yml is empty)
dbt seed                                   # load taxi_zone_lookup.csv → DuckDB

dbt run                                    # all layers
dbt run --select staging                   # staging only
dbt run --select intermediate              # intermediate only
dbt run --select marts                     # marts only

# Override input path for local dev
dbt run --vars '{"input_dir": "/absolute/path/to/input"}'

dbt test                                   # run all schema tests
dbt docs generate && dbt docs serve        # browse lineage at localhost:8080
```

**5. Run ad-hoc queries**
```bash
duckdb nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
duckdb nyc_taxi.duckdb < queries/q2_hour_of_day_pattern.sql
duckdb nyc_taxi.duckdb < queries/q3_consecutive_gap_analysis.sql
```

---

### Airflow on Docker

**1. Start all services**
```bash
docker compose up -d
```

First boot installs dbt + DuckDB via `_PIP_ADDITIONAL_REQUIREMENTS` — allow ~60–90 seconds.

**2. Wait for readiness**
```bash
docker compose logs -f airflow-init   # wait for: "Admin user created"
docker compose logs -f webserver      # wait for: "Airflow is ready"
```

**3. Open the UI**
```
http://localhost:8080
Username: admin
Password: admin
```

**4. Enable and trigger the DAG**

UI → DAGs → `nyc_taxi_daily_pipeline` → toggle on → Trigger DAG ▶

**5. Monitor**
```bash
docker compose logs -f scheduler      # live scheduler output
```

**6. Shut down**
```bash
docker compose down          # stops containers, preserves DuckDB volume
docker compose down -v       # full teardown including all volumes
```

---

### PySpark Backfill (optional)

```bash
spark-submit spark/process_historical.py \
  --input_dir  input/ \
  --output_dir input/processed/ \
  --year       2023
```

Output is Parquet partitioned by `year` / `month`, compatible with the staging glob pattern.

---

## 15. Project Structure

```
.
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_yellow_trips.sql      # Renames, casts, trip_id, duration
│   │   │   ├── stg_taxi_zones.sql        # Renames, zone_clean, is_airport flag
│   │   │   ├── sources.yml               # Source declarations + freshness config
│   │   │   └── schema.yml                # Staging tests (trip_id not_null)
│   │   ├── intermediate/
│   │   │   └── int_trips_enriched.sql    # Zone joins + quality filters + derived cols
│   │   └── marts/
│   │       ├── fct_trips.sql             # Core fact table (pass-through)
│   │       ├── dim_zones.sql             # Zone dimension — all 265 rows
│   │       ├── agg_daily_metrics.sql     # Daily volume + revenue rollup
│   │       ├── agg_zone_performance.sql  # Zone ranking + tip rate + speed
│   │       └── schema.yml                # Mart tests (location_id unique+not_null)
│   ├── seeds/
│   │   └── taxi_zone_lookup.csv          # TLC zone reference (265 rows)
│   ├── tests/                            # Custom singular tests
│   ├── profiles.yml                      # DuckDB connection (Docker paths)
│   ├── profiles.yml.example              # Template with env_var() pattern
│   └── dbt_project.yml                   # Materialisations, quoting, vars
│
├── dags/
│   └── nyc_taxi_daily_pipeline.py        # 8-task Airflow DAG (02:00 UTC)
│
├── queries/
│   ├── q1_top_zones_by_revenue.sql       # Top 10 zones: GROUP BY + LIMIT
│   ├── q2_hour_of_day_pattern.sql        # 24-hour spine: generate_series + LEFT JOIN
│   └── q3_consecutive_gap_analysis.sql   # Inter-trip gap: LAG() window function
│
├── spark/
│   └── process_historical.py             # PySpark schema normalisation + partitioning
│
├── input/                                # Raw Parquet + CSV (gitignored)
├── docker-compose.yml                    # Airflow stack: scheduler + webserver + postgres
├── requirements.txt                      # Pinned Python deps
├── TECHNICAL_GUIDE.md                    # Deep-dive: design decisions, interview Q&A
└── .gitignore                            # Excludes *.duckdb, target/, .venv/, input/
```

---

## 16. Future Improvements

**Incremental models** — highest priority. Converting `int_trips_enriched` to `materialized='incremental'` with `unique_key='trip_id'` reduces daily build time from a full 35M-row scan to processing only new files.

**Atomic schema swap** — implement the blue/green promotion pattern described in [Section 13](#13-brainstormer-blue-green-deployment) before any production deployment.

**Extended data quality** — add `dbt-expectations` for statistical tests (value range assertions, distribution checks), relationship tests between `fct_trips.pickup_location_id` and `dim_zones.location_id`, and `accepted_values` tests on `payment_type` and `rate_code_id`.

**Source freshness via dbt** — replace the Python `check_source_freshness` task with `dbt source freshness` as a dedicated Airflow task, configured per-source with `warn_after` and `error_after` SLAs.

**Alerting** — add `on_failure_callback` to `DEFAULT_ARGS` for Slack/PagerDuty notifications. Add anomaly detection on `agg_daily_metrics`: alert if `total_trips` for the latest date deviates more than 2σ from the 30-day rolling mean.

**CI/CD** — GitHub Actions pipeline: on every PR, run `dbt seed` + `dbt run` + `dbt test` against a sample DuckDB instance. Block merge on any test failure.

**Multi-year coverage** — parameterise year in `stg_yellow_trips` so the glob covers 2019–present without SQL changes. Add `stg_green_trips` and `stg_fhv_trips` for full TLC coverage.

**Custom schema naming** — add a `generate_schema_name` dbt macro to produce clean schema names (`marts` instead of `main_marts`), eliminating the need to explain the `main_` prefix to downstream consumers.

---

*For architecture deep-dives, design decision rationale, and full interview preparation, see [TECHNICAL_GUIDE.md](TECHNICAL_GUIDE.md).*
