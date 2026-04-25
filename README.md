# 🏗️ NYC Taxi Lakehouse Pipeline

An enterprise-grade, cloud-native data lakehouse pipeline built on AWS, processing **20M+ NYC Yellow Taxi records** through a medallion architecture (Bronze → Silver → Gold), orchestrated by Apache Airflow and queryable via Amazon Athena.

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│              NYC TLC Yellow Taxi (Parquet, ~300MB)              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Raw Landing Zone (S3)                         │
│         s3://lakehouse-raw-landing/nyc_taxi/yellow/2024/        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│               Bronze Layer  (PySpark + S3)                      │
│  • Explicit schema enforcement (StructType)                     │
│  • Metadata columns: _ingested_at, _source_system, _source_path │
│  • Partition by year/month                                      │
│  • Idempotent writes (partitionOverwriteMode=dynamic)           │
│  • Lightweight quality gate (null rate threshold)               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│               Silver Layer  (PySpark + S3)                      │
│  • Bad record quarantine with dq_issues column                  │
│  • Deduplication via ROW_NUMBER() window function               │
│  • Broadcast join enrichment with Taxi Zone lookup (265 rows)   │
│  • Column standardisation (snake_case, derived metrics)         │
│  • Partition by year/month                                      │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│               Gold Layer  (PySpark + S3)                        │
│  • daily_revenue_by_zone  — Revenue KPIs per zone per day       │
│  • hourly_demand          — Peak hour demand analysis           │
│  • payment_type_summary   — Revenue by payment method           │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│          AWS Glue Catalog + Amazon Athena                       │
│  • Glue Crawler auto-discovers partitions                       │
│  • SQL analytics over Gold layer via Athena                     │
└─────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow (Dockerised)
pipeline_start → bronze_ingestion → silver_transformation →
gold_aggregations → run_glue_crawler → pipeline_complete
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Data Processing | Apache Spark 3.3.4 (PySpark) |
| Storage | AWS S3 (Medallion Architecture) |
| Orchestration | Apache Airflow 2.7.3 (Docker) |
| Metadata Catalog | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Infrastructure | AWS IAM, S3, Glue, Athena |
| Language | Python 3.11 |

---

## 📁 Project Structure

```
nyc-taxi-lakehouse/
├── ingestion/
│   ├── bronze_ingestion.py        # Raw ingestion with schema enforcement
│   ├── silver_transformation.py   # Cleansing, dedup & enrichment
│   └── gold_aggregations.py       # Business KPI aggregations
├── dags/
│   └── lakehouse_pipeline.py      # Airflow DAG definition
├── infra/
│   └── docker-compose.yml         # Airflow + Docker setup
├── scripts/
│   ├── run_bronze_local.sh        # Local Bronze job runner
│   ├── run_silver_local.sh        # Local Silver job runner
│   └── run_gold_local.sh          # Local Gold job runner
└── README.md
```

---

## 🗂️ Data Pipeline Details

### Bronze Layer
- Reads raw Parquet files from S3 landing zone
- Enforces explicit `StructType` schema — prevents silent type coercions
- Adds lineage metadata: `_ingested_at`, `_source_system`, `_source_path`
- Partitions output by `_year` / `_month` for downstream pruning
- Idempotent writes — safe to re-run without duplicating data

### Silver Layer
- Filters bad records using business rules (date range, negative amounts, null locations)
- Quarantines rejected records with tagged `dq_issues` column for full auditability
- Deduplicates using `ROW_NUMBER()` window function over deterministic key — avoids non-deterministic `dropDuplicates()`
- Enriches with Taxi Zone lookup via **broadcast join** (265-row table) — eliminates shuffle on 20M row dataset
- Derives `trip_duration_minutes` and standardises column names to snake_case

### Gold Layer
- **daily_revenue_by_zone**: Total trips, revenue, avg fare per pickup zone per day
- **hourly_demand**: Trip volume by hour of day with weekday/weekend segmentation
- **payment_type_summary**: Revenue breakdown by payment method with tip rate analysis
- Silver dataset cached in memory — reused across all three Gold aggregations

---

## 📊 Dataset

| Metric | Value |
|---|---|
| Source | NYC TLC Yellow Taxi Trip Records |
| Period | January – June 2024 |
| Raw Records | ~20.3 Million |
| Valid Records (Silver) | ~20.1 Million (98.7%) |
| Quarantined Records | ~263K (1.3%) |
| Raw Data Size | ~300MB |

---

## ⚙️ Setup & Running

### Prerequisites
- Python 3.11
- Apache Spark 3.3.4
- AWS Account with S3, Glue, Athena access
- Docker Desktop
- AWS CLI configured (`aws configure`)

### 1. Clone the repository
```bash
git clone https://github.com/sivaganesha/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
```

### 2. Install Python dependencies
```bash
pip install pyspark==3.3.4 boto3
```

### 3. Download Hadoop AWS JARs
```bash
curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.1026.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar
```

### 4. Configure AWS credentials
```bash
aws configure
```

### 5. Update S3 bucket names
Update the bucket name constants in each ingestion script to match your S3 buckets.

### 6. Download NYC Taxi data
Download Yellow Taxi Parquet files from [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and upload to your raw landing bucket:
```bash
aws s3 cp yellow_tripdata_2024-01.parquet \
  s3://your-raw-landing-bucket/nyc_taxi/yellow/2024/
```

### 7. Start Airflow
```bash
cd infra
docker compose up -d
```

### 8. Trigger the pipeline
Open `http://localhost:8080`, log in with `airflow/airflow`, and trigger the `lakehouse_medallion_pipeline` DAG.

---

## 🔍 Querying with Athena

After the pipeline completes, query the Gold layer via Athena:

```sql
-- Top 10 zones by revenue
SELECT pickup_zone, pickup_borough,
       SUM(total_trips) AS trips,
       ROUND(SUM(total_revenue), 2) AS revenue
FROM lakehouse_gold.daily_revenue_by_zone
WHERE _year = '2024' AND _month = '1'
GROUP BY pickup_zone, pickup_borough
ORDER BY revenue DESC
LIMIT 10;
```

```sql
-- Peak hours on weekdays
SELECT pickup_hour, day_of_week_name,
       total_trips, avg_revenue_per_trip
FROM lakehouse_gold.hourly_demand
WHERE is_weekend = false AND _year = '2024'
ORDER BY total_trips DESC
LIMIT 10;
```

---

## 🎯 Key Design Decisions

| Decision | Rationale |
|---|---|
| Explicit StructType schema | Catches upstream data drift early — fails fast on type changes |
| Quarantine over drop | Full auditability — every rejected record has a tagged reason |
| ROW_NUMBER() for dedup | Deterministic tiebreaking — `dropDuplicates()` is non-deterministic |
| Broadcast join for zones | 265-row lookup table — eliminates shuffle on 20M row dataset |
| partitionOverwriteMode=dynamic | Idempotent writes — only overwrites affected partitions on re-run |
| Silver cached for Gold | Single S3 read — reused across 3 Gold aggregations |
| Glue Crawler in pipeline | Auto-discovers new partitions — no manual MSCK REPAIR needed |

---

## 📈 Pipeline Performance

| Layer | Records | Duration |
|---|---|---|
| Bronze | 20.3M records ingested | ~8 mins |
| Silver | 20.1M valid + 263K quarantined | ~25 mins |
| Gold | 3 KPI tables aggregated | ~15 mins |

---

## 🚀 Production Considerations

This pipeline is **cloud-ready**. To migrate from local to AWS EMR Serverless:
1. Change `--master local[*]` to EMR Serverless application ARN
2. Update credential provider from hardcoded keys to IAM roles
3. Submit jobs via AWS CLI or Step Functions instead of Airflow BashOperator

---

## 👤 Author

**Siva Ganesh**
- GitHub: [@sivaganesha](https://github.com/sivaganesha)
