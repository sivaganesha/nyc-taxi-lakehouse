"""
=============================================================
Gold Layer — Business Aggregations & KPI Tables
=============================================================
Reads from Silver layer and produces three Gold tables:

  1. daily_revenue_by_zone  — revenue, trips, avg fare per
                              pickup zone per day
  2. hourly_demand          — trip volume by hour of day
                              (peak hour analysis)
  3. payment_type_summary   — revenue breakdown by payment type

Design decisions:
  - Gold tables are wide, denormalised, query-optimised
  - Partitioned by year/month for Athena cost efficiency
  - Column names are business-friendly (no underscores prefix)
  - Aggregations use approx_count_distinct where exactness
    is not critical (faster on large datasets)
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
AWS_REGION          = "eu-north-1"
AWS_ACCESS_KEY      = ""
AWS_SECRET_KEY      = ""

SILVER_BUCKET       = "s3a://lakehouse-silver-siva"
GOLD_BUCKET         = "s3a://lakehouse-gold-siva"

SILVER_INPUT_PATH   = f"{SILVER_BUCKET}/nyc_taxi/yellow/"

# Gold table output paths
DAILY_REVENUE_PATH  = f"{GOLD_BUCKET}/nyc_taxi/daily_revenue_by_zone/"
HOURLY_DEMAND_PATH  = f"{GOLD_BUCKET}/nyc_taxi/hourly_demand/"
PAYMENT_SUMMARY_PATH= f"{GOLD_BUCKET}/nyc_taxi/payment_type_summary/"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("LakehouseGoldAggregations")
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
        # Gold aggregations produce small output — fewer shuffle partitions
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.hadoop.fs.s3a.connection.timeout", "300000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/tmp/spark-events")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver(spark: SparkSession, path: str):
    """
    Read Silver Parquet.
    Cache it since we derive multiple Gold tables from the same dataset —
    avoids re-reading from S3 for each aggregation.
    """
    print(f"[Gold] Reading Silver data from: {path}")
    df = spark.read.format("parquet").load(path)
    #df.cache()
    count = df.count()
    print(f"[Gold] Silver record count (cached): {count:,}")
    return df


def build_daily_revenue_by_zone(df):
    """
    Gold Table 1: Daily Revenue by Pickup Zone

    Aggregates per pickup_zone + pickup_date:
      - total_trips
      - total_revenue
      - avg_fare
      - avg_trip_distance
      - avg_trip_duration_minutes
      - total_passengers

    Business use: identify high-revenue zones by day,
    power zone-level dashboards, feed pricing models.
    """
    print("[Gold] Building daily_revenue_by_zone...")

    agg_df = (
        df
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .groupBy(
            "pickup_date",
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            "_year",
            "_month"
        )
        .agg(
            F.count("*")                          .alias("total_trips"),
            F.round(F.sum("total_amount"), 2)     .alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2)    .alias("avg_trip_distance_miles"),
            F.round(F.avg("trip_duration_minutes"),2).alias("avg_trip_duration_mins"),
            F.sum("passenger_count")              .alias("total_passengers"),
            F.round(F.max("total_amount"), 2)     .alias("max_single_fare"),
        )
        .withColumn("processed_at",
                    F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .orderBy("pickup_date", F.desc("total_revenue"))
    )

    print(f"[Gold] daily_revenue_by_zone rows: {agg_df.count():,}")
    return agg_df


def build_hourly_demand(df):
    """
    Gold Table 2: Hourly Demand Pattern

    Aggregates trip volume by hour of day across the dataset.
    Useful for:
      - Identifying peak hours (surge pricing triggers)
      - Driver supply planning
      - Comparing weekday vs weekend demand

    Uses day_of_week (1=Sunday, 7=Saturday in Spark).
    """
    print("[Gold] Building hourly_demand...")

    agg_df = (
        df
        .withColumn("pickup_hour",       F.hour("pickup_datetime"))
        .withColumn("day_of_week_num",   F.dayofweek("pickup_datetime"))
        .withColumn("day_of_week_name",
                    F.when(F.dayofweek("pickup_datetime") == 1, "Sunday")
                     .when(F.dayofweek("pickup_datetime") == 2, "Monday")
                     .when(F.dayofweek("pickup_datetime") == 3, "Tuesday")
                     .when(F.dayofweek("pickup_datetime") == 4, "Wednesday")
                     .when(F.dayofweek("pickup_datetime") == 5, "Thursday")
                     .when(F.dayofweek("pickup_datetime") == 6, "Friday")
                     .otherwise("Saturday"))
        .withColumn("is_weekend",
                    F.dayofweek("pickup_datetime").isin([1, 7]))
        .groupBy(
            "pickup_hour",
            "day_of_week_num",
            "day_of_week_name",
            "is_weekend",
            "_year",
            "_month"
        )
        .agg(
            F.count("*")                       .alias("total_trips"),
            F.round(F.avg("total_amount"), 2)  .alias("avg_revenue_per_trip"),
            F.round(F.avg("trip_distance"), 2) .alias("avg_distance_miles"),
            F.round(F.avg("passenger_count"), 1).alias("avg_passengers"),
        )
        .withColumn("processed_at",
                    F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .orderBy("day_of_week_num", "pickup_hour")
    )

    print(f"[Gold] hourly_demand rows: {agg_df.count():,}")
    return agg_df


def build_payment_type_summary(df):
    """
    Gold Table 3: Payment Type Revenue Summary

    payment_type codes (TLC standard):
      1 = Credit card
      2 = Cash
      3 = No charge
      4 = Dispute
      5 = Unknown
      6 = Voided trip

    Business use: revenue attribution, fraud detection
    (high dispute rates), cash vs card trend analysis.
    """
    print("[Gold] Building payment_type_summary...")

    agg_df = (
        df
        .withColumn("payment_type_desc",
                    F.when(F.col("payment_type") == 1, "Credit Card")
                     .when(F.col("payment_type") == 2, "Cash")
                     .when(F.col("payment_type") == 3, "No Charge")
                     .when(F.col("payment_type") == 4, "Dispute")
                     .when(F.col("payment_type") == 5, "Unknown")
                     .when(F.col("payment_type") == 6, "Voided Trip")
                     .otherwise("Other"))
        .groupBy(
            "payment_type",
            "payment_type_desc",
            "_year",
            "_month"
        )
        .agg(
            F.count("*")                          .alias("total_trips"),
            F.round(F.sum("total_amount"), 2)     .alias("total_revenue"),
            F.round(F.avg("total_amount"), 2)     .alias("avg_fare"),
            F.round(F.sum("tip_amount"), 2)       .alias("total_tips"),
            F.round(F.avg("tip_amount"), 2)       .alias("avg_tip"),
        )
        .withColumn("tip_rate_pct",
                    F.round(
                        F.col("total_tips") / F.col("total_revenue") * 100, 2))
        .withColumn("processed_at",
                    F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .orderBy(F.desc("total_revenue"))
    )

    print(f"[Gold] payment_type_summary rows: {agg_df.count():,}")
    return agg_df


def write_gold(df, output_path: str, table_name: str):
    """
    Write Gold table partitioned by year/month.
    Gold tables are small (aggregated) so fewer part files are needed.
    coalesce(1) per partition keeps file count minimal for Athena efficiency.
    """
    print(f"[Gold] Writing {table_name} to: {output_path}")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("_year", "_month")
        .save(output_path)
    )
    print(f"[Gold] ✅ {table_name} write complete.")


def main():
    spark = create_spark_session()

    try:
        # 1. Read & cache Silver
        silver_df = read_silver(spark, SILVER_INPUT_PATH)

        # 2. Build Gold tables
        daily_revenue_df    = build_daily_revenue_by_zone(silver_df)
        hourly_demand_df    = build_hourly_demand(silver_df)
        payment_summary_df  = build_payment_type_summary(silver_df)

        # 3. Write Gold tables
        write_gold(daily_revenue_df,   DAILY_REVENUE_PATH,   "daily_revenue_by_zone")
        write_gold(hourly_demand_df,   HOURLY_DEMAND_PATH,   "hourly_demand")
        write_gold(payment_summary_df, PAYMENT_SUMMARY_PATH, "payment_type_summary")

        # 4. Preview each table
        print("\n[Gold] === daily_revenue_by_zone (top 5) ===")
        daily_revenue_df.select(
            "pickup_date", "pickup_zone", "pickup_borough",
            "total_trips", "total_revenue", "avg_fare"
        ).show(5, truncate=False)

        print("\n[Gold] === hourly_demand (top 5 by trips) ===")
        hourly_demand_df.orderBy(F.desc("total_trips")).select(
            "day_of_week_name", "pickup_hour",
            "total_trips", "avg_revenue_per_trip", "is_weekend"
        ).show(5, truncate=False)

        print("\n[Gold] === payment_type_summary ===")
        payment_summary_df.select(
            "payment_type_desc", "total_trips",
            "total_revenue", "avg_tip", "tip_rate_pct"
        ).show(10, truncate=False)

    finally:
        silver_df.unpersist()
        spark.stop()
        print("[Gold] Spark session stopped.")


if __name__ == "__main__":
    main()
