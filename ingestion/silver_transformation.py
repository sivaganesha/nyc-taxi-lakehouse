"""
=============================================================
Silver Layer — Cleansing, Deduplication & Enrichment Job
=============================================================
Reads from Bronze layer, applies:
  1. Schema casting & standardisation
  2. Bad record filtering (timestamp range, negative fares)
  3. Deduplication using window functions
  4. Enrichment via broadcast join with Taxi Zone lookup
  5. Data quality flag columns
  6. Writes partitioned Parquet to Silver bucket

Key design decisions:
  - Bad records are QUARANTINED, not silently dropped
  - Broadcast join for zone lookup (small table ~265 rows)
  - Dedup uses ROW_NUMBER() over a deterministic window
  - Silver schema is strict — nulls in critical cols are rejected
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
AWS_REGION          = "eu-north-1"
AWS_ACCESS_KEY      = ""
AWS_SECRET_KEY      = ""

BRONZE_BUCKET       = "s3a://lakehouse-bronze-siva"
SILVER_BUCKET       = "s3a://lakehouse-silver-siva"
ZONES_PATH          = "s3a://lakehouse-raw-landing-siva/nyc_taxi/zones/taxi_zone_lookup.csv"

BRONZE_INPUT_PATH   = f"{BRONZE_BUCKET}/nyc_taxi/yellow/"
SILVER_OUTPUT_PATH  = f"{SILVER_BUCKET}/nyc_taxi/yellow/"
QUARANTINE_PATH     = f"{SILVER_BUCKET}/nyc_taxi/yellow_quarantine/"

# Valid date range for NYC taxi data
MIN_PICKUP_YEAR     = 2019
MAX_PICKUP_YEAR     = 2025


# ─────────────────────────────────────────────
# ZONE LOOKUP SCHEMA
# ─────────────────────────────────────────────
ZONE_SCHEMA = StructType([
    StructField("LocationID",  IntegerType(), True),
    StructField("Borough",     StringType(),  True),
    StructField("Zone",        StringType(),  True),
    StructField("service_zone",StringType(),  True),
])


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("LakehouseSilverTransformation")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.shuffle.partitions", "8")
        # Enable broadcast join for small tables
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
#        .config("spark.eventLog.enabled", "true")
 #       .config("spark.eventLog.dir", "/tmp/spark-events")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark: SparkSession, path: str):
    """Read Bronze Parquet — all partitions."""
    print(f"[Silver] Reading Bronze data from: {path}")
    df = spark.read.format("parquet").load(path)
    print(f"[Silver] Bronze record count: {df.count():,}")
    return df


def read_zone_lookup(spark: SparkSession, path: str):
    """
    Read Taxi Zone CSV and broadcast it.
    At 265 rows this is a textbook broadcast join candidate —
    avoids a shuffle entirely when joining with millions of records.
    """
    print(f"[Silver] Reading Zone lookup from: {path}")
    zone_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(ZONE_SCHEMA)
        .load(path)
    )
    print(f"[Silver] Zone lookup record count: {zone_df.count():,}")
    return zone_df


def cast_and_standardise(df):
    """
    Standardise column names and types.
    Bronze lands data as-is; Silver enforces the canonical schema.
    - Rename camelCase TLC columns to snake_case
    - Derive trip_duration_minutes from timestamps
    - Cast store_and_fwd_flag to boolean
    """
    print("[Silver] Casting and standardising columns...")
    df = (
        df
        .withColumnRenamed("VendorID",              "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID",            "rate_code_id")
        .withColumnRenamed("PULocationID",          "pickup_location_id")
        .withColumnRenamed("DOLocationID",          "dropoff_location_id")
        # Derive trip duration
        .withColumn("trip_duration_minutes",
                    F.round(
                        (F.unix_timestamp("dropoff_datetime") -
                         F.unix_timestamp("pickup_datetime")) / 60.0,
                        2))
        # Standardise flag column to boolean
        .withColumn("store_and_fwd_flag",
                    F.when(F.col("store_and_fwd_flag") == "Y", True)
                     .otherwise(False)
                     .cast(BooleanType()))
        # Normalise passenger_count nulls to 0
        .withColumn("passenger_count",
                    F.coalesce(F.col("passenger_count").cast(IntegerType()),
                               F.lit(0)))
    )
    return df


def split_good_bad_records(df):
    """
    Tag records as GOOD or BAD based on business rules.
    Bad records are quarantined — never silently dropped.
    This preserves auditability: you can always explain
    exactly why a record was excluded.

    Rules:
      - Pickup year must be in valid range
      - trip_distance must be >= 0
      - total_amount must be >= 0
      - pickup_location_id and dropoff_location_id must not be null
      - trip_duration must be > 0 (dropoff after pickup)
    """
    print("[Silver] Applying data quality rules...")

    df = df.withColumn(
        "dq_issues",
        F.concat_ws(", ",
            F.when(
                ~F.year("pickup_datetime").between(MIN_PICKUP_YEAR, MAX_PICKUP_YEAR),
                F.lit(f"pickup_year_out_of_range({MIN_PICKUP_YEAR}-{MAX_PICKUP_YEAR})")
            ),
            F.when(F.col("trip_distance") < 0,
                   F.lit("negative_trip_distance")),
            F.when(F.col("total_amount") < 0,
                   F.lit("negative_total_amount")),
            F.when(F.col("pickup_location_id").isNull(),
                   F.lit("null_pickup_location")),
            F.when(F.col("dropoff_location_id").isNull(),
                   F.lit("null_dropoff_location")),
            F.when(F.col("trip_duration_minutes") <= 0,
                   F.lit("non_positive_duration")),
        )
    ).withColumn("is_valid", F.col("dq_issues") == "")

    good_df = df.filter(F.col("is_valid")).drop("dq_issues", "is_valid")
    bad_df  = df.filter(~F.col("is_valid"))

    good_count = good_df.count()
    bad_count  = bad_df.count()
    total      = good_count + bad_count

    print(f"  ✅ Valid records:      {good_count:,} ({good_count/total:.1%})")
    print(f"  ❌ Quarantined records: {bad_count:,} ({bad_count/total:.1%})")

    return good_df, bad_df


def deduplicate(df):
    """
    Remove duplicate trips using ROW_NUMBER() window function.

    Dedup key: (pickup_datetime, dropoff_datetime,
                pickup_location_id, dropoff_location_id, total_amount)

    Deterministic tiebreaker: vendor_id (prefer vendor 1 over 2).
    This is the Silver-layer pattern for dedup —
    avoids dropDuplicates() which is non-deterministic on tiebreaks.
    """
    print("[Silver] Deduplicating records...")

    window = Window.partitionBy(
        "pickup_datetime", "dropoff_datetime",
        "pickup_location_id", "dropoff_location_id",
        "total_amount"
    ).orderBy("vendor_id")

    df = (
        df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    print(f"[Silver] Record count after dedup: {df.count():,}")
    return df


def enrich_with_zones(df, zone_df):
    """
    Broadcast join with Taxi Zone lookup for pickup and dropoff zones.

    Why broadcast?
      - Zone table is 265 rows (~12KB) — fits in memory on every executor
      - Avoids a shuffle of the 3M row taxi table entirely
      - You can verify in Spark UI: look for BroadcastHashJoin in the plan

    We join twice — once for pickup, once for dropoff — with
    column renaming to avoid ambiguity.
    """
    print("[Silver] Enriching with Taxi Zone lookup (broadcast join)...")

    # Prepare zone lookup with prefixed columns to avoid conflicts
    pickup_zones  = zone_df.select(
        F.col("LocationID").alias("pickup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone")
    )

    dropoff_zones = zone_df.select(
        F.col("LocationID").alias("dropoff_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone")
    )

    # Broadcast both zone lookups
    df = (
        df
        .join(F.broadcast(pickup_zones),  on="pickup_location_id",  how="left")
        .join(F.broadcast(dropoff_zones), on="dropoff_location_id", how="left")
    )

    print("[Silver] Zone enrichment complete.")
    return df


def add_silver_metadata(df):
    """Add Silver layer processing metadata columns."""
    processed_at = datetime.utcnow().isoformat()
    df = (
        df
        .withColumn("_silver_processed_at", F.lit(processed_at).cast(TimestampType()))
        .withColumn("_silver_version",       F.lit("1.0"))
        # Partition columns
        .withColumn("_year",  F.year("pickup_datetime").cast(StringType()))
        .withColumn("_month", F.lpad(
                        F.month("pickup_datetime").cast(StringType()), 2, "0"))
    )
    return df


def write_silver(df, output_path: str):
    """Write cleansed, enriched data to Silver bucket."""
    print(f"[Silver] Writing Silver data to: {output_path}")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("_year", "_month")
        .save(output_path)
    )
    print("[Silver] ✅ Silver write complete.")


def write_quarantine(df, quarantine_path: str):
    """
    Write bad records to quarantine zone with dq_issues column intact.
    These can be inspected, fixed, and reprocessed later.
    """
    if df.count() == 0:
        print("[Silver] No quarantine records to write.")
        return
    print(f"[Silver] Writing quarantine records to: {quarantine_path}")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .save(quarantine_path)
    )
    print("[Silver] ✅ Quarantine write complete.")


def main():
    spark = create_spark_session()

    try:
        # 1. Read Bronze
        bronze_df = read_bronze(spark, BRONZE_INPUT_PATH)

        # 2. Cast & standardise
        standardised_df = cast_and_standardise(bronze_df)

        # 3. Split good / bad records
        good_df, bad_df = split_good_bad_records(standardised_df)

        # 4. Deduplicate good records
        deduped_df = deduplicate(good_df)

        # 5. Read zone lookup & enrich
        zone_df    = read_zone_lookup(spark, ZONES_PATH)
        enriched_df = enrich_with_zones(deduped_df, zone_df)

        # 6. Add Silver metadata
        silver_df = add_silver_metadata(enriched_df)

        # 7. Write Silver & quarantine
        write_silver(silver_df, SILVER_OUTPUT_PATH)
        write_quarantine(bad_df, QUARANTINE_PATH)

        # 8. Sample output
        print("\n[Silver] Sample output (5 rows):")
        silver_df.select(
            "pickup_datetime", "dropoff_datetime",
            "trip_duration_minutes", "total_amount",
            "pickup_zone", "pickup_borough",
            "dropoff_zone", "_silver_processed_at"
        ).show(5, truncate=False)

    finally:
        spark.stop()
        print("[Silver] Spark session stopped.")


if __name__ == "__main__":
    main()
