"""
=============================================================
Bronze Layer — Raw Ingestion Job
=============================================================
Reads NYC Yellow Taxi Parquet files from S3 raw-landing zone,
adds metadata columns, enforces schema, partitions by year/month,
and writes to the Bronze S3 bucket.

Key design decisions:
  - Explicit StructType schema → catches upstream data drift early
  - Metadata columns → full lineage traceability
  - Partition by year/month → efficient predicate pushdown downstream
  - mode("overwrite") per partition → idempotent, safe to re-run
=============================================================
"""

import sys
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, FloatType,
    StringType, TimestampType
)

# ─────────────────────────────────────────────
# CONFIG — update these to match your S3 setup
# ─────────────────────────────────────────────
AWS_REGION          = "eu-north-1"
RAW_LANDING_BUCKET  = "s3a://lakehouse-raw-landing-siva"
BRONZE_BUCKET       = "s3a://lakehouse-bronze-siva"
SOURCE_SYSTEM       = "nyc_tlc"
INPUT_PATH          = f"{RAW_LANDING_BUCKET}/nyc_taxi/yellow/2024/"
OUTPUT_PATH         = f"{BRONZE_BUCKET}/nyc_taxi/yellow/"


# ─────────────────────────────────────────────
# SCHEMA — explicit definition for Yellow Taxi
# Prevents silent type coercions from inferSchema
# ─────────────────────────────────────────────
YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID",                IntegerType(),      True),
    StructField("tpep_pickup_datetime",    TimestampType(), True),
    StructField("tpep_dropoff_datetime",   TimestampType(), True),
    StructField("passenger_count",         LongType(),    True),
    StructField("trip_distance",           DoubleType(),    True),
    StructField("RatecodeID",              LongType(),    True),
    StructField("store_and_fwd_flag",      StringType(),    True),
    StructField("PULocationID",            IntegerType(),      True),
    StructField("DOLocationID",            IntegerType(),      True),
    StructField("payment_type",            LongType(),      True),
    StructField("fare_amount",             DoubleType(),    True),
    StructField("extra",                   DoubleType(),    True),
    StructField("mta_tax",                 DoubleType(),    True),
    StructField("tip_amount",              DoubleType(),    True),
    StructField("tolls_amount",            DoubleType(),    True),
    StructField("improvement_surcharge",   DoubleType(),    True),
    StructField("total_amount",            DoubleType(),    True),
    StructField("congestion_surcharge",    DoubleType(),    True),
    StructField("airport_fee",             DoubleType(),    True),
])


def create_spark_session() -> SparkSession:
    """
    Build SparkSession with S3 (hadoop-aws) configuration.
    Uses AWS credentials from environment variables —
    never hardcode keys in code.
    """
    spark = (
        SparkSession.builder
        .appName("LakehouseBronzeIngestion")
        .master("local[*]")  # use all local cores; change to spark:// for cluster
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "")
        .config("spark.hadoop.fs.s3a.secret.key", "")
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        # Performance tuning for S3 reads
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")       # 100MB multipart
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Shuffle partition tuning — NYC taxi monthly file ~50MB parquet
        # 200 default partitions is too high for local; 8 is reasonable
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/tmp/spark-events")
       
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, path: str):
    """
    Read Parquet files from raw landing zone with explicit schema.
    mergeSchema=true handles minor schema drift across monthly files.
    """
    print(f"[Bronze] Reading raw data from: {path}")
    df = (
        spark.read
        .format("parquet")
        .option("mergeSchema", "true")
        .schema(YELLOW_TAXI_SCHEMA)
        .load(path)
    )
    print(f"[Bronze] Raw record count: {df.count():,}")
    return df


def add_metadata_columns(df, source_path: str):
    """
    Add lineage and audit columns to every Bronze record.
    These columns are critical for:
      - Tracing records back to source files
      - Auditing when data arrived in the lake
      - Filtering by ingestion date in downstream jobs
    """
    ingested_at = datetime.utcnow().isoformat()

    df = (
        df
        .withColumn("_ingested_at",    F.lit(ingested_at).cast(TimestampType()))
        .withColumn("_source_system",  F.lit(SOURCE_SYSTEM))
        .withColumn("_source_path",    F.lit(source_path))
        # Partition columns derived from pickup datetime
        .withColumn("_year",  F.year(F.col("tpep_pickup_datetime")).cast(StringType()))
        .withColumn("_month", F.lpad(
                        F.month(F.col("tpep_pickup_datetime")).cast(StringType()),
                        2, "0"))   # zero-pad: 1 → "01", 12 → "12"
    )
    return df


def validate_basic_quality(df):
    """
    Lightweight quality gate at Bronze layer.
    Bronze is NOT the place for heavy cleansing — that's Silver.
    But we fail fast on catastrophic issues:
      - Null rate > 20% on critical columns
      - Zero records
    """
    total = df.count()
    if total == 0:
        raise ValueError("[Bronze] FATAL: Zero records read from source. Aborting.")

    critical_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                     "PULocationID", "DOLocationID", "total_amount"]

    print("[Bronze] Running basic quality checks...")
    for col_name in critical_cols:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_rate  = null_count / total
        status     = "❌ WARN" if null_rate > 0.20 else "✅ OK"
        print(f"  {status}  {col_name}: {null_rate:.2%} nulls ({null_count:,} / {total:,})")

        if null_rate > 0.20:
            raise ValueError(
                f"[Bronze] Quality gate FAILED: {col_name} has {null_rate:.2%} nulls. "
                f"Threshold is 20%. Investigate source data before proceeding."
            )

    print(f"[Bronze] Quality checks passed. Total records: {total:,}")
    return df


def write_bronze(df, output_path: str):
    """
    Write to Bronze bucket partitioned by year and month.

    Partition strategy rationale:
      - year/month aligns with source file cadence (monthly drops)
      - Downstream Silver jobs process one month at a time
      - Enables efficient partition pruning in Athena/Spark queries

    partitionOverwriteMode=dynamic ensures only affected partitions
    are overwritten on re-runs — not the entire table.
    """
    print(f"[Bronze] Writing to: {output_path}")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("_year", "_month")
        .save(output_path)
    )
    print("[Bronze] ✅ Write complete.")


def main():
    spark = create_spark_session()

    try:
        # 1. Read raw data
        raw_df = read_raw(spark, INPUT_PATH)

        # 2. Add metadata / lineage columns
        df_with_meta = add_metadata_columns(raw_df, INPUT_PATH)

        # 3. Basic quality gate
        validated_df = validate_basic_quality(df_with_meta)

        # 4. Write to Bronze
        write_bronze(validated_df, OUTPUT_PATH)

        # 5. Print sample for verification
        print("\n[Bronze] Sample output (5 rows):")
        validated_df.select(
            "tpep_pickup_datetime", "PULocationID", "DOLocationID",
            "total_amount", "_ingested_at", "_source_system", "_year", "_month"
        ).show(5, truncate=False)

    finally:
        spark.stop()
        print("[Bronze] Spark session stopped.")


if __name__ == "__main__":
    main()
