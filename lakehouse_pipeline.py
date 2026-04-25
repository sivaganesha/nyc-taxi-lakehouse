"""
=============================================================
Lakehouse Pipeline DAG
=============================================================
Orchestrates the full medallion pipeline:
  Bronze Ingestion → Silver Transformation → Gold Aggregations

Each task runs the corresponding PySpark job via BashOperator.
If any task fails, downstream tasks are automatically skipped.

Schedule: @daily (runs once per day)
Can also be triggered manually from the Airflow UI.
=============================================================
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# DAG DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    "owner":            "siva",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# PATHS (inside Docker container)
# ─────────────────────────────────────────────
SPARK_HOME      = "/opt/spark"
SPARK_SUBMIT    = f"{SPARK_HOME}/bin/spark-submit"
PROJECT_DIR     = "/opt/lakehouse"
PYTHON_BIN      = "/opt/homebrew/bin/python3.11"

# Common spark-submit options
SPARK_CONF = (
    "--master local[*] "
    "--driver-memory 2g "
    "--conf spark.sql.shuffle.partitions=8 "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf 'spark.hadoop.fs.s3a.endpoint=s3.eu-north-1.amazonaws.com' "
)


def log_pipeline_start(**context):
    """Log pipeline start metadata to Airflow XCom for traceability."""
    run_id    = context["run_id"]
    exec_date = context["execution_date"]
    print(f"[Pipeline] Starting Lakehouse Pipeline")
    print(f"[Pipeline] Run ID:          {run_id}")
    print(f"[Pipeline] Execution Date:  {exec_date}")
    print(f"[Pipeline] Triggered by:    {context['dag_run'].run_type}")
    return {"status": "started", "run_id": run_id}


def log_pipeline_complete(**context):
    """Log pipeline completion."""
    print(f"[Pipeline] ✅ Lakehouse Pipeline completed successfully!")
    print(f"[Pipeline] All three layers (Bronze/Silver/Gold) are up to date.")


# ─────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────
with DAG(
    dag_id="lakehouse_medallion_pipeline",
    description="End-to-end NYC Taxi Lakehouse: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,           # Don't backfill historical runs
    max_active_runs=1,       # Only one run at a time
    tags=["lakehouse", "nyc-taxi", "pyspark", "s3"],
) as dag:

    # ── Task 0: Log pipeline start ──
    pipeline_start = PythonOperator(
        task_id="pipeline_start",
        python_callable=log_pipeline_start,
        provide_context=True,
    )

    # ── Task 1: Bronze Ingestion ──
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            f"export PYSPARK_PYTHON={PYTHON_BIN} && "
            f"export PYSPARK_DRIVER_PYTHON={PYTHON_BIN} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} "
            f"{PROJECT_DIR}/ingestion/bronze_ingestion.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 2: Silver Transformation ──
    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            f"export PYSPARK_PYTHON={PYTHON_BIN} && "
            f"export PYSPARK_DRIVER_PYTHON={PYTHON_BIN} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} "
            f"{PROJECT_DIR}/ingestion/silver_transformation.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 3: Gold Aggregations ──
    gold_aggregations = BashOperator(
        task_id="gold_aggregations",
        bash_command=(
            f"export PYSPARK_PYTHON={PYTHON_BIN} && "
            f"export PYSPARK_DRIVER_PYTHON={PYTHON_BIN} && "
            f"{SPARK_SUBMIT} {SPARK_CONF} "
            f"{PROJECT_DIR}/ingestion/gold_aggregations.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 4: Log pipeline complete ──
    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_complete,
        provide_context=True,
    )

    # ─────────────────────────────────────────────
    # TASK DEPENDENCIES
    # Bronze → Silver → Gold (strict sequential)
    # If Bronze fails, Silver and Gold are skipped
    # ─────────────────────────────────────────────
    pipeline_start >> bronze_ingestion >> silver_transformation >> gold_aggregations >> pipeline_complete
