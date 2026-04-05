"""
process_historical.py
---------------------
PySpark job for back-filling / processing large volumes of historical
NYC Taxi Parquet files before they are handed off to the dbt pipeline.

Responsibilities:
  - Read multiple monthly Parquet files from INPUT_DIR
  - Apply lightweight schema normalization (column renames, type casts)
  - Write a consolidated, partitioned Parquet dataset to OUTPUT_DIR

This script is intentionally kept free of business logic — that lives
in dbt.  Spark is used only where the data volume justifies distributed
processing (e.g., multi-year historical loads).

Usage:
  spark-submit spark/process_historical.py \
    --input_dir  /data/raw/yellow_taxi \
    --output_dir /data/processed/yellow_taxi \
    --year       2023

Environment tested against: PySpark 3.5.x, Python 3.11
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column normalisation map: raw name -> canonical name
# (extend as TLC schema evolves across years)
# ---------------------------------------------------------------------------
COLUMN_RENAMES: dict[str, str] = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "RatecodeID": "rate_code_id",
    "VendorID": "vendor_id",
}

# Columns we keep after normalisation (drop the rest for storage efficiency)
COLUMNS_TO_KEEP: list[str] = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "congestion_surcharge",
]


def build_spark_session(app_name: str = "nyc_taxi_historical") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Tune based on your cluster; these are local-mode defaults
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def normalise_schema(df):
    """Rename raw TLC columns to canonical names and cast to expected types."""
    for raw, canonical in COLUMN_RENAMES.items():
        if raw in df.columns:
            df = df.withColumnRenamed(raw, canonical)

    # Enforce types — TLC files are occasionally inconsistent across months
    df = (
        df
        .withColumn("pickup_datetime",    F.col("pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime",   F.col("dropoff_datetime").cast(TimestampType()))
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast(IntegerType()))
        .withColumn("dropoff_location_id",F.col("dropoff_location_id").cast(IntegerType()))
        .withColumn("trip_distance",      F.col("trip_distance").cast(DoubleType()))
        .withColumn("total_amount",       F.col("total_amount").cast(DoubleType()))
    )
    return df


def add_partition_columns(df):
    """Add year/month columns derived from pickup_datetime for Hive-style partitioning."""
    return (
        df
        .withColumn("year",  F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )


def filter_invalid_rows(df):
    """
    Drop rows that are structurally invalid.
    Business-level anomaly detection belongs in dbt tests, not here.
    """
    return df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & (F.col("total_amount") >= 0)
        & F.col("pickup_location_id").isNotNull()
    )


def run(input_dir: str, output_dir: str, year: int) -> None:
    spark = build_spark_session()
    logger.info("Spark session started. Reading from: %s (year=%s)", input_dir, year)

    # Read all monthly Parquet files for the requested year
    pattern = str(Path(input_dir) / f"yellow_tripdata_{year}-*.parquet")
    raw_df = spark.read.parquet(pattern)
    logger.info("Raw row count: %d", raw_df.count())

    df = (
        raw_df
        .transform(normalise_schema)
        .transform(filter_invalid_rows)
        .transform(add_partition_columns)
    )

    # Keep only known columns (drop anything not in our whitelist)
    available = [c for c in COLUMNS_TO_KEEP + ["year", "month"] if c in df.columns]
    df = df.select(available)

    logger.info("Processed row count: %d", df.count())

    # Write partitioned output — downstream dbt reads from here
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_dir)
    )
    logger.info("Written to: %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="NYC Taxi historical Spark processor")
    parser.add_argument("--input_dir",  required=True, help="Directory with raw Parquet files")
    parser.add_argument("--output_dir", required=True, help="Destination directory for processed output")
    parser.add_argument("--year",       required=True, type=int, help="Calendar year to process")
    args = parser.parse_args()

    run(args.input_dir, args.output_dir, args.year)
