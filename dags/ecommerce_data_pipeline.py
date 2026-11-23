"""
DAG: ecommerce_data_engineering_pipeline
Description: End-to-end orchestration pipeline for the Olist Brazilian E-Commerce
             Data Warehouse project. Orchestrates ingestion validation, PySpark
             data cleaning & transformation, Parquet persistence, star schema
             modeling, idempotent PostgreSQL warehouse loading, and SQL business
             analytics execution.
Schedule: None (Manual trigger for reproducible portfolio execution)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Ensure project root (/opt/airflow or local path) is in sys.path
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
if AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)

# Pipeline task callables importing existing project modules
def task_validate_raw_data():
    """Validates raw Olist CSV datasets against expected schemas and profiles dimensions."""
    from src.ingestion.ingest_raw import run_raw_ingestion_validation
    success = run_raw_ingestion_validation()
    if not success:
        raise ValueError("Raw data validation failed! Check data/analytics/raw_data_profile.txt.")
    return "Raw data validation passed."


def task_transform_olist_data():
    """Executes PySpark data cleaning, date parsing, SLA calculations, and Parquet persistence."""
    from src.transformation.clean_data import run_pipeline
    success = run_pipeline()
    if not success:
        raise ValueError("PySpark data transformation or data quality validation failed!")
    return "PySpark transformation completed successfully."


def task_validate_processed_data():
    """Validates that all 8 processed Parquet datasets exist and meet required row count baselines."""
    import pyarrow.parquet as pq

    processed_dir = os.path.join(AIRFLOW_HOME, "data", "processed")
    expected_tables = {
        "customers.parquet": 99441,
        "geolocation.parquet": 19015,
        "order_items.parquet": 112650,
        "order_payments.parquet": 103886,
        "order_reviews.parquet": 99224,
        "orders.parquet": 99441,
        "products.parquet": 32951,
        "sellers.parquet": 3095,
    }

    for table_name, expected_count in expected_tables.items():
        table_path = os.path.join(processed_dir, table_name)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Processed table missing: {table_path}")
        table = pq.read_table(table_path)
        actual_count = table.num_rows
        if actual_count != expected_count:
            raise ValueError(
                f"Row count mismatch in {table_name}: expected {expected_count}, got {actual_count}"
            )

    return "All 8 processed Parquet datasets validated."


def task_build_star_schema():
    """Transforms processed Parquet datasets into Kimball Star Schema dimension and fact DataFrames."""
    from src.transformation.build_star_schema import execute_build_star_schema
    tables = execute_build_star_schema()
    if not tables:
        raise ValueError("Failed to build star schema tables!")
    return "Star schema constructed and saved to data/processed/warehouse/."


def task_load_postgres_warehouse():
    """Loads star schema tables into PostgreSQL data warehouse with idempotent TRUNCATE & COPY."""
    from src.warehouse.postgres_loader import load_star_schema
    # Use recreate_ddl=False for idempotent truncate & reload
    results = load_star_schema(recreate_ddl=False)
    expected_total = 349405
    actual_total = sum(results.values())
    if actual_total != expected_total:
        raise ValueError(
            f"Warehouse row count mismatch: expected {expected_total:,}, loaded {actual_total:,}"
        )
    return f"Successfully loaded {actual_total:,} rows into PostgreSQL data warehouse."


def task_run_sql_analytics():
    """Executes all 7 analytical SQL queries, benchmarks execution, and exports CSV results."""
    from src.warehouse.run_analytics import run_all_analytics
    success = run_all_analytics()
    if not success:
        raise ValueError("SQL analytics execution or validation checks failed!")
    return "All 7 SQL business analytics queries executed and validated."


def task_validate_analytics_outputs():
    """Validates that all analytical CSV exports and audit report exist and are populated."""
    results_dir = os.path.join(AIRFLOW_HOME, "data", "analytics", "sql_results")
    report_file = os.path.join(AIRFLOW_HOME, "data", "analytics", "sql_analytics_report.txt")

    expected_csvs = [
        "01_monthly_revenue_growth.csv",
        "02_top_product_categories.csv",
        "03_delivery_performance.csv",
        "04_customer_rfm_segmentation.csv",
        "05_payment_type_distribution.csv",
        "06_state_sales_performance.csv",
        "07_seller_performance.csv",
    ]

    for csv_name in expected_csvs:
        csv_path = os.path.join(results_dir, csv_name)
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            raise FileNotFoundError(f"Missing or empty analytical CSV export: {csv_path}")

    if not os.path.exists(report_file) or os.path.getsize(report_file) == 0:
        raise FileNotFoundError(f"Missing or empty analytics audit report: {report_file}")

    with open(report_file, "r", encoding="utf-8") as f:
        content = f.read()
        if "FINAL PHASE 5 VERDICT: PASS" not in content:
            raise ValueError("SQL analytics audit report did not indicate PASS status!")

    return "All derived analytical outputs verified."


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="ecommerce_data_engineering_pipeline",
    default_args=default_args,
    description="End-to-end ETL, Kimball Star Schema DW, and SQL Analytics pipeline for Olist E-Commerce",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "olist", "pyspark", "postgres", "data_warehouse"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=task_validate_raw_data,
    )

    transform_olist_data = PythonOperator(
        task_id="transform_olist_data",
        python_callable=task_transform_olist_data,
    )

    validate_processed_data = PythonOperator(
        task_id="validate_processed_data",
        python_callable=task_validate_processed_data,
    )

    build_star_schema = PythonOperator(
        task_id="build_star_schema",
        python_callable=task_build_star_schema,
    )

    load_postgres_warehouse = PythonOperator(
        task_id="load_postgres_warehouse",
        python_callable=task_load_postgres_warehouse,
    )

    run_sql_analytics = PythonOperator(
        task_id="run_sql_analytics",
        python_callable=task_run_sql_analytics,
    )

    validate_analytics_outputs = PythonOperator(
        task_id="validate_analytics_outputs",
        python_callable=task_validate_analytics_outputs,
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
    )

    # Define strict sequential dependency flow
    (
        start
        >> validate_raw_data
        >> transform_olist_data
        >> validate_processed_data
        >> build_star_schema
        >> load_postgres_warehouse
        >> run_sql_analytics
        >> validate_analytics_outputs
        >> pipeline_complete
    )
