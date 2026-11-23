# Architecture & Data Engineering Design Document

## 1. Executive Summary
This project implements an end-to-end, production-grade Data Engineering and Analytics Pipeline based on the Brazilian E-Commerce Public Dataset by Olist (~100k orders from 2016 to 2018).

The pipeline extracts raw e-commerce CSVs, performs distributed cleansing and dimensional modeling using Apache Spark (PySpark), conducts rigorous data quality checks, loads the structured data into a PostgreSQL Data Warehouse adhering to a Kimball Star Schema, executes analytical SQL queries, and is orchestrated end-to-end via Apache Airflow in Docker containers.

---

## 2. End-to-End Architecture Flow

```
[ Kaggle / Raw Olist CSVs (data/raw/) ]
             │
             ▼
[ 1. Ingestion & Raw Validation ] ─── Validates 9 CSV files, schemas, encodings, and row count tolerances
             │
             ▼
[ 2. PySpark Processing Engine ] 
     ├── Data Cleansing (Null handling, timestamp normalization, geolocation deduplication)
     ├── Referential Integrity & Data Quality Suite (All assertions passed)
     ├── Surrogate Key Generation (Deterministic integer keys)
     └── Kimball Star Schema Generation (Parquet layer in data/processed/warehouse/)
             │
             ▼
[ 3. Processed Quality Gate ] ────── Asserts Parquet schemas, non-empty datasets, column types
             │
             ▼
[ 4. PostgreSQL Data Warehouse ] ─── Dedicated Database: olist_dw (Port 5432, schema: dwh)
     ├── Dimensions: dim_date (1,827), dim_customer (99,441), dim_product (32,951), dim_seller (3,095)
     ├── Facts: fact_order (99,441), fact_order_item (112,650)
     └── Total Warehouse Records: 349,405 (100% reconciled, 0 PK/FK violations)
             │
             ▼
[ 5. SQL Business Analytics ] ────── 7 Analytical KPI queries exported to data/analytics/sql_results/
             ▲
             │ (Orchestrated by)
[ 6. Apache Airflow (Docker) ] ───── Multi-container stack (Airflow 2.7.3 + Java 17 + PySpark 3.5.9)
     ├── Dedicated Metadata DB: airflow_metadata (Port 5433)
     ├── Web UI: http://localhost:8080
     └── DAG: ecommerce_data_engineering_pipeline (9 sequential tasks)
```

---

## 3. Dimensional Data Model (Star Schema)

The data warehouse uses a Kimball-style dimensional star schema optimized for fast analytical aggregations, reporting, and BI tool integration.

### Fact Tables
1. **`dwh.fact_order`** (Grain: 1 row per order, 99,441 rows)
   - Primary Key: `order_key` (INTEGER surrogate key)
   - Natural Key: `order_id` (VARCHAR(50) UNIQUE)
   - Foreign Keys: `customer_key` (FK -> `dim_customer`), `order_date_key` (FK -> `dim_date`), delivery date keys
   - Measures: `order_item_count`, `total_price`, `total_freight`, `total_order_value`, `total_payment`, `review_score`, `delivery_time_days`, `delivery_delay_days`
   - Attributes: `order_status`, `is_delayed`

2. **`dwh.fact_order_item`** (Grain: 1 row per line item within an order, 112,650 rows)
   - Primary Key: `order_item_key` (INTEGER surrogate key)
   - Natural Key: `(order_id, order_item_id)` (Composite UNIQUE)
   - Foreign Keys: `customer_key`, `product_key`, `seller_key`, `order_date_key`
   - Measures: `price`, `freight_value`, `order_item_total`

### Dimension Tables
1. **`dwh.dim_customer`** (99,441 rows)
   - Primary Key: `customer_key` (INTEGER surrogate key)
   - Natural Key: `customer_id` (VARCHAR(50) UNIQUE)
   - Attributes: `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`

2. **`dwh.dim_product`** (32,951 rows)
   - Primary Key: `product_key` (INTEGER surrogate key)
   - Natural Key: `product_id` (VARCHAR(50) UNIQUE)
   - Attributes: `product_category_name`, `product_category_name_english`, dimensions, weight

3. **`dwh.dim_seller`** (3,095 rows)
   - Primary Key: `seller_key` (INTEGER surrogate key)
   - Natural Key: `seller_id` (VARCHAR(50) UNIQUE)
   - Attributes: `seller_zip_code_prefix`, `seller_city`, `seller_state`

4. **`dwh.dim_date`** (1,827 rows, 2016–2020 calendar days)
   - Primary Key: `date_key` (INTEGER, YYYYMMDD format)
   - Attributes: `full_date`, `day_of_week`, `day_name`, `day_of_month`, `month`, `month_name`, `quarter`, `year`, `is_weekend`

---

## 4. Airflow Containerization & Orchestration Architecture

The orchestration tier is containerized via Docker Compose with strict service separation:

```
+----------------------------------------------------------------------------------------------------+
|                                      DOCKER COMPOSE NETWORK                                        |
|                                                                                                    |
|  +---------------------------+    Heartbeat / Task State    +----------------------------------+   |
|  |     airflow-webserver     |<---------------------------->|        airflow-scheduler         |   |
|  | (Flask UI on Host :8080)  |                              | (LocalExecutor, PySpark Worker)  |   |
|  +---------------------------+                              +----------------------------------+   |
|                │                                                             │                     |
|                │ Connections / ORM Queries                                   │ Execution & Logging |
|                ▼                                                             ▼                     |
|  +---------------------------+                              +----------------------------------+   |
|  | airflow-postgres-metadata |                              |        olist-postgres-dw         |   |
|  | (Port 5433 -> DWH isolated|                              | (Port 5432, Dedicated Warehouse) |   |
|  +---------------------------+                              +----------------------------------+   |
+----------------------------------------------------------------------------------------------------+
```

### Key Orchestration Design Principles:
1. **Dedicated Database Isolation**: Airflow metadata lives in `airflow-postgres-metadata` on port 5433 (`airflow_metadata` database). The analytical warehouse lives in `olist-postgres-dw` on port 5432 (`olist_dw` database). Neither system shares connections, tables, or locking mechanisms.
2. **Custom Multi-Stage Docker Image**: Built from `apache/airflow:2.7.3-python3.10` with OpenJDK 17 (`/usr/lib/jvm/java-17-openjdk-amd64`), `procps`, PySpark 3.5.9, and pinned `SQLAlchemy<2.0.0,>=1.4.28` for stable Airflow 2.7 ORM compatibility.
3. **VM Memory Safety**: Airflow webserver workers are constrained to 1 (`AIRFLOW__WEBSERVER__WORKERS: '1'`), and Spark memory is allocated with `SPARK_DRIVER_MEMORY=512m` and `SPARK_EXECUTOR_MEMORY=512m`, ensuring the entire stack runs comfortably within 3.3 GB Docker Desktop VM limits without triggering OOM errors.
4. **Idempotent Linear Pipeline**: The DAG `ecommerce_data_engineering_pipeline` executes 9 tasks in strict topological order:
   `start >> validate_raw_data >> transform_olist_data >> validate_processed_data >> build_star_schema >> load_postgres_warehouse >> run_sql_analytics >> validate_analytics_outputs >> pipeline_complete`.

---

## 5. Pipeline Stages & Modules

| Stage | Path / Module | Purpose |
|---|---|---|
| **Raw Validation** | `src/validation/raw_data_validator.py` | Validates file existence, row count bounds, and column schemas for all 9 raw CSVs |
| **Transform** | `src/transformation/clean_data.py` | PySpark data cleaning, date parsing, geolocation deduplication, and DQ gate |
| **Processed Validation** | `src/validation/processed_data_validator.py` | Asserts processed Parquet schema validity, column types, and non-empty outputs |
| **Star Schema Modeling** | `src/transformation/build_star_schema.py` | Kimball dimensional modeling, surrogate key generation, writes warehouse Parquets |
| **Warehouse Loading** | `src/warehouse/postgres_loader.py` | Atomically truncates tables and streams 349,405 rows into PostgreSQL via COPY |
| **Warehouse Validation** | `src/warehouse/validate_warehouse.py` | 25 automated DQ assertions, PK/FK checks, and financial measure reconciliation |
| **SQL Analytics** | `src/analytics/run_analytics.py` | Executes 7 SQL business queries and exports CSV artifacts to `data/analytics/` |
| **Analytics Validation** | `src/validation/analytics_validator.py` | Verifies all analytical CSV outputs exist, are non-empty, and contain correct headers |
| **Airflow DAG** | `dags/ecommerce_data_pipeline.py` | End-to-end automated orchestration pipeline with execution monitoring |
