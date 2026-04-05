# nyc_taxi_daily_pipeline.py

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,  # ✅ FIXED
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
INPUT_DIR = os.getenv("INPUT_DIR", "/opt/airflow/data/input")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/nyc_taxi.duckdb")

DBT_BASE = "dbt --no-use-colors"


# ✅ RENAMED + IMPROVED (freshness check)
def check_source_freshness(**context) -> None:
    import glob
    from datetime import datetime, timedelta

    execution_date = context["ds"]
    target_date = datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)

    expected_pattern = f"{INPUT_DIR}/yellow_tripdata_{target_date.strftime('%Y-%m')}*.parquet"

    files = glob.glob(expected_pattern)

    print(f"Checking files for: {target_date.strftime('%Y-%m')}")
    print(f"Pattern: {expected_pattern}")
    print(f"Files found: {len(files)}")

    if not files:
        raise FileNotFoundError(f"No data files found for {target_date.strftime('%Y-%m')}")

    print("Source freshness check passed")


with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="NYC Taxi Pipeline",
    schedule="0 2 * * *",  # ✅ FIXED (02:00 UTC)
    start_date=datetime(2024, 1, 1),
    catchup=True,  # ✅ FIXED (backfill support)
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "dbt", "duckdb"],
) as dag:

    # ✅ RENAMED TASK
    t_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
    )

    # dbt deps (optional but kept)
    t_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE} deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    # dbt seed
    t_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"set -e; {DBT_BASE} seed "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # staging
    t_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"set -e; {DBT_BASE} run --select staging "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # intermediate
    t_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"set -e; {DBT_BASE} run --select intermediate "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # marts
    t_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"set -e; {DBT_BASE} run --select marts "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # tests (fail DAG if fails)
    t_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"set -e; {DBT_BASE} test "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ✅ IMPROVED SUCCESS LOGGING
    t_notify = BashOperator(
        task_id="notify_success",
        bash_command=f"""
        python - <<EOF
import duckdb

conn = duckdb.connect("{DUCKDB_PATH}")

result = conn.execute(\"\"\"
SELECT 
    COUNT(*) as trips,
    SUM(revenue) as revenue
FROM main_marts.fct_trips
\"\"\").fetchall()

print(f"SUCCESS: Trips={{result[0][0]}}, Revenue={{result[0][1]}}")
EOF
        """,
    )

    # ✅ UPDATED DEPENDENCY CHAIN
    t_freshness >> t_deps >> t_seed >> t_staging >> t_intermediate >> t_marts >> t_tests >> t_notify