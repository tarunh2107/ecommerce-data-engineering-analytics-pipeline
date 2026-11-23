# 🛒 E-Commerce Data Engineering & Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

> An end-to-end, enterprise-grade Data Engineering and Business Intelligence platform built with **Python 3.11, PySpark 3.5, PostgreSQL 15, Docker, Apache Airflow 2.7, and Streamlit**. It ingests, cleans, models, reconciles, and visualizes 1.25M+ records across 9 relational datasets from the **Olist Brazilian E-Commerce public dataset**.

---

## 📌 Table of Contents

- [Project Overview & Business Problem](#-project-overview--business-problem)
- [System Architecture](#-system-architecture)
- [Datasets Description & Citation](#-datasets-description--citation)
- [Data Pipeline & Distributed Processing (PySpark)](#-data-pipeline--distributed-processing-pyspark)
- [Data Quality & Reconciliation Gate](#-data-quality--reconciliation-gate)
- [PostgreSQL Data Warehouse & Kimball Star Schema](#-postgresql-data-warehouse--kimball-star-schema)
- [Advanced SQL Analytics & Business Insights](#-advanced-sql-analytics--business-insights)
- [Pipeline Orchestration (Apache Airflow & Docker)](#-pipeline-orchestration-apache-airflow--docker)
- [Interactive Analytics Platform (Streamlit)](#-interactive-analytics-platform-streamlit)
- [Repository Structure](#-repository-structure)
- [Installation & Execution Guide](#-installation--execution-guide)
- [Production Verification & Reconciliation Results](#-production-verification--reconciliation-results)
- [Future Enhancements](#-future-enhancements)
- [Author & License](#-author--license)

---

## 🎯 Project Overview & Business Problem

Operating a marketplace platform with thousands of active merchants, hundreds of thousands of customers, and multi-state logistics creates distributed, messy data across order fulfillment, product catalog, delivery tracking, customer reviews, and multi-tender payments.

Direct analytical queries against transactional OLTP databases cause severe database locking, slow report generation, and calculation inconsistencies (e.g., fan-out traps from joining 1-to-many payment and order-item records).

### The Solution:
This project delivers a robust, automated Lakehouse/Warehouse platform:
1. **Raw Layer (Bronze)**: Ingests 9 raw CSV datasets (1.25M+ rows) with strict schema validation.
2. **Processed Layer (Silver)**: Cleans, deduplicates, parses timestamps, handles nulls, and structures data into columnar **Snappy-compressed Parquet**.
3. **Data Quality Gate**: Enforces 12 automated integrity checks (uniqueness, referential integrity, range thresholds) preventing invalid data from reaching production.
4. **Data Warehouse Layer (Gold)**: Models data into a **Kimball Star Schema** in PostgreSQL with surrogate keys, referential constraints, and optimized b-tree indexes (349K+ records).
5. **Business Analytics Layer**: Employs advanced SQL (CTEs, Window Functions, RFM Segmentation, Pareto Analysis) to derive actionable strategic insights.
6. **Airflow Orchestration**: Coordinates a 9-task automated DAG inside custom Docker containers.
7. **Presentation Layer**: Deploys a full-featured, multi-page **Streamlit** dashboard directly querying the PostgreSQL warehouse with millisecond caching.

---

## 🏗️ System Architecture

```
                                  +-------------------------------------------------+
                                  |     Raw Ingestion Layer (Olist E-Commerce)      |
                                  |         9 CSV Files (~136 MB, 1.25M+ rows)      |
                                  +------------------------+------------------------+
                                                           |
                                                           v
                                  +-------------------------------------------------+
                                  |          Distributed ETL Layer (PySpark)        |
                                  |  - Schema enforcement, date casting, imputation |
                                  |  - Surrogate key generation via MD5 hashing     |
                                  |  - Storage: Snappy Parquet (Processed Layer)    |
                                  +------------------------+------------------------+
                                                           |
                                                           v
                                  +-------------------------------------------------+
                                  |            Automated Quality Gate               |
                                  |  - Null checks, PK uniqueness, FK integrity     |
                                  |  - Value range assertions, orphan detection     |
                                  +------------------------+------------------------+
                                                           |
                                                           v
                                  +-------------------------------------------------+
                                  |        PostgreSQL Data Warehouse (Docker)       |
                                  |                 Schema: `dwh`                   |
                                  |  - Dimensions: customers, products, sellers,    |
                                  |               date                              |
                                  |  - Facts: fact_orders, fact_order_items         |
                                  |  - 349,405 warehouse records, R$ 0.00 drift     |
                                  +-------------------+-----------------------------+
                                                      |
                         +----------------------------+----------------------------+
                         |                                                         |
                         v                                                         v
+------------------------------------------------+       +------------------------------------------------+
|       Apache Airflow Orchestration (Docker)    |       |      Interactive Analytics App (Streamlit)     |
|  - 9-task deterministic DAG                    |       |  - 7 multi-page analytical modules             |
|  - Idempotent table truncations and reloading  |       |  - Executive KPI summary, RFM, Pareto          |
|  - Automated execution logging & alerts        |       |  - Interactive Plotly charts & caching         |
+------------------------------------------------+       +------------------------------------------------+
```

---

## 📂 Datasets Description & Citation

The pipeline processes 9 relational datasets from the **Brazilian E-Commerce Public Dataset by Olist**, originally published on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

### Dataset Source & Usage Terms
- **Source**: Olist and André Sionek via [Kaggle Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
- **License / Terms**: Released under the **Creative Commons CC BY-NC-SA 4.0** (Attribution-NonCommercial-ShareAlike 4.0 International) license.
- **Context**: This open dataset contains ~100,000 real-world commercial orders made at Olist Store between 2016 and 2018 across Brazilian marketplaces. Customer, seller, and partner details have been anonymized to protect personal identification while preserving authentic transactional, logistic, and temporal dynamics.
- **Permitted Usage**: Free for academic, educational, portfolio, and non-commercial research purposes with proper attribution.

| Dataset | Record Count | Description |
|---|---|---|
| `olist_orders_dataset.csv` | 99,441 | Order identifiers, customer links, order status, and lifecycle timestamps |
| `olist_order_items_dataset.csv` | 112,650 | Item-level details, prices, freight values, seller references, shipping limits |
| `olist_order_payments_dataset.csv` | 103,886 | Payment methods, installment breakdowns, and transaction values |
| `olist_order_reviews_dataset.csv` | 99,224 | Customer review scores (1-5 stars) and feedback timestamps |
| `olist_customers_dataset.csv` | 99,441 | Customer identifiers, unique customer UUIDs, cities, and states |
| `olist_products_dataset.csv` | 32,951 | Product dimensions, categories, weight, and volume metrics |
| `olist_sellers_dataset.csv` | 3,095 | Seller identifiers, locations, zip code prefixes, cities, and states |
| `olist_geolocation_dataset.csv` | 1,000,163 | Brazilian zip codes, latitude, longitude, and administrative regions |
| `product_category_name_translation.csv` | 71 | Portuguese-to-English translation mapping for product categories |

---

## ⚙️ Data Pipeline & Distributed Processing (PySpark)

Data transformations are implemented with **PySpark 3.5**, leveraging memory caching, column projections, and vectorized processing:

1. **Schema Casting & Standardization**: Standardizes string dates into ISO-8601 timestamps (`yyyy-MM-dd HH:mm:ss`), casts monetary columns to double/decimal, and strips trailing whitespaces.
2. **Missing Value Treatment**:
   - Missing product categories mapped to `'unknown'`.
   - Dimension attributes filled with meaningful defaults (`'N/A'` or regional medians).
   - Order delivery delays derived: `is_delayed = order_delivered_customer_date > order_estimated_delivery_date`.
3. **Surrogate Key Generation**:
   - Deterministic surrogate keys generated using MD5 hashing over business natural keys (`customer_id`, `product_id`, `seller_id`, `date`), eliminating auto-increment lock contention during parallel data loading.
4. **Columnar Parquet Storage**:
   - Cleaned tables and dimensional tables are persisted to `data/processed/` and `data/processed/warehouse/` in Snappy-compressed Parquet format, achieving an ~80% compression ratio compared to raw CSVs.

---

## 🛡️ Data Quality & Reconciliation Gate

Before loading into the PostgreSQL data warehouse, the pipeline executes an automated quality validation suite (`src/quality/data_quality_checks.py`):

- **Primary Key Uniqueness**: Confirms 0 duplicates on primary and surrogate keys across all dimensions and facts.
- **Null Constraint Validations**: Ensures critical business fields (`order_id`, `customer_key`, `price`, `order_status`) have 0% missing values.
- **Referential Integrity**: Verifies 100% of foreign keys in `fact_orders` and `fact_order_items` resolve to existing dimension records (zero orphan rows).
- **Financial Measure Reconciliation**:
  - Reconciles total item price and freight sum against source staging tables.
  - Target: **R$ 15,422,461.53** Gross Merchandise Value.
  - Achieved financial variance: **R$ 0.00 (0.00% discrepancy)**.

---

## 🏛️ PostgreSQL Data Warehouse & Kimball Star Schema

The warehouse resides in PostgreSQL 15 under a dedicated `dwh` schema, structured as a **Kimball Star Schema**:

```
                            +--------------------+
                            |    dim_sellers     |
                            +--------------------+
                            | PK seller_key      |
                            |    seller_id       |
                            |    seller_city     |
                            |    seller_state    |
                            +---------+----------+
                                      |
+--------------------+                |                +--------------------+
|   dim_customers    |                |                |    dim_products    |
+--------------------+                |                +--------------------+
| PK customer_key    |                |                | PK product_key     |
|    customer_id     |<---------+     |     +--------->|    product_id      |
|    customer_city   |          |     |     |          |    category_name   |
|    customer_state  |          |     |     |          |    weight / size   |
+--------------------+          |     |     |          +--------------------+
                                |     |     |
                          +-----+-----+-----+----+
                          |   fact_order_items   |
                          +----------------------+
                          | PK order_item_key    |
                          | FK order_id          |
                          | FK customer_key      |
                          | FK product_key       |
                          | FK seller_key        |
                          | FK order_date_key    |
                          |    price             |
                          |    freight_value     |
                          |    total_item_value  |
                          +----------+-----------+
                                     |
                                     |
                          +----------+-----------+     +--------------------+
                          |      fact_orders     |     |      dim_date      |
                          +----------------------+     +--------------------+
                          | PK order_key         |     | PK date_key        |
                          |    order_id (UK)     |     |    full_date       |
                          | FK customer_key      +---->|    year, month     |
                          | FK order_date_key    +---->|    quarter, day    |
                          | FK delivered_date_key+---->|    day_of_week     |
                          |    total_order_value |     |    is_weekend      |
                          |    delivery_days     |     +--------------------+
                          |    is_delayed        |
                          |    review_score      |
                          +----------------------+
```

### Table Specifications:

| Table Name | Grain | Type | Verified Records |
|---|---|---|---|
| `dwh.dim_customers` | One row per customer order location | Dimension | 99,441 |
| `dwh.dim_products` | One row per catalog SKU | Dimension | 32,951 |
| `dwh.dim_sellers` | One row per marketplace seller | Dimension | 3,095 |
| `dwh.dim_date` | One row per calendar day (2016-2018) | Dimension | 1,826 |
| `dwh.fact_orders` | One row per customer order | Fact | 99,441 |
| `dwh.fact_order_items` | One row per line item inside an order | Fact | 112,650 |
| **Total Warehouse Records** | | | **349,405** |

---

## 📈 Advanced SQL Analytics & Business Insights

The analytics suite (`sql/analytics/`) queries the PostgreSQL warehouse directly using CTEs, window functions (`LAG()`, `NTILE()`, `ROW_NUMBER()`, `DENSE_RANK()`), and aggregations:

1. **Monthly Revenue & MoM Growth (`01_monthly_revenue_growth.sql`)**:
   - Uses `LAG()` window function over calendar months to compute MoM percentage change and rolling revenue.
   - *Insight*: Sales peaked in November 2017 (Black Friday) reaching over R$ 1.19M in monthly GMV.
2. **Top Product Categories (`02_top_product_categories.sql`)**:
   - Aggregates category volume, revenue share, and customer review scores using `DENSE_RANK()`.
   - *Insight*: Health & Beauty, Watches & Gifts, and Bed/Bath/Table drive over 30% of marketplace revenue.
3. **Logistics & Delivery Performance (`03_delivery_performance.sql`)**:
   - Calculates delivery SLAs, actual vs. estimated days, and customer satisfaction correlation.
   - *Insight*: On-time orders average a **4.29 / 5.00** review rating, whereas delayed orders plummet to **2.57 / 5.00**.
4. **Customer RFM Segmentation (`04_customer_rfm_segmentation.sql`)**:
   - Employs `NTILE(5)` window functions across **Recency** (days since last purchase), **Frequency** (total orders), and **Monetary** value (total spend) to classify customers into behavioral tiers (*Champions, Loyal, Potential Loyalists, At Risk, Hibernating*).
5. **Payment Method Analysis (`05_payment_type_distribution.sql`)**:
   - Evaluates payment method distribution, installment usage, and average order value.
   - *Insight*: Credit card payments represent 73.9% of transactions and 78.3% of total GMV.
6. **State-Level Sales Performance (`06_state_sales_performance.sql`)**:
   - Evaluates geographic sales density, interstate logistics freight ratios, and revenue per state.
   - *Insight*: São Paulo (SP) dominates both buyer and seller geographic distributions (>40% of orders).
7. **Seller Performance & Pareto Analysis (`07_seller_performance.sql`)**:
   - Ranks merchant contribution and cumulative revenue distribution.
   - *Insight*: Confirms the Pareto principle—**the top 20% of sellers generate 82.06% of platform revenue**.

---

## ⏱️ Pipeline Orchestration (Apache Airflow & Docker)

The complete end-to-end pipeline is automated through **Apache Airflow 2.7.3** running in an isolated Docker container environment:

### DAG Flow (`ecommerce_data_pipeline`):
```
start 
  >> validate_raw_data 
  >> transform_olist_data 
  >> validate_processed_data 
  >> build_star_schema 
  >> load_postgres_warehouse 
  >> run_sql_analytics 
  >> validate_analytics_outputs 
  >> pipeline_complete
```

- **Containerized Architecture**: Custom Docker image with OpenJDK 17 and PySpark pre-installed, mounting local workspace directories.
- **Port Isolation**: PostgreSQL warehouse running on standard host port `5432`; Airflow metadata PostgreSQL running on isolated internal port `5433` to prevent database namespace collision.
- **Idempotency**: All table loading operations utilize transactional truncations (`TRUNCATE ... RESTART IDENTITY`) and PostgreSQL `COPY` batch streaming, guaranteeing identical results on repeated DAG runs.

---

## 🖥️ Interactive Analytics Platform (Streamlit)

The user-facing presentation layer is an interactive web platform built with **Streamlit** and **Plotly**, connecting directly to the PostgreSQL data warehouse schema (`dwh`):

```bash
# Launch the Streamlit Analytics Platform
streamlit run dashboard/app.py --server.port 8502
```
- **Local Application URL**: `http://localhost:8502`

### Key Platform Modules:
1. **Executive Overview**: Executive KPI scoreboard (Total Orders, Total GMV, AOV, Active Sellers, Average Rating), monthly GMV trend with MoM growth, and category revenue drivers.
2. **Sales Analytics**: Deep-dive sales performance with dynamic **Year** and **Customer State** filters, daily/monthly revenue trends, and basket sizes.
3. **Product Analytics**: Category volume, unit pricing distribution, English translated categories, and dynamic **Top 10 / 20 / 30** display toggles.
4. **Customer Analytics**: Behavioral RFM cohort segmentation with interactive tier summaries and customer-level drilldowns.
5. **Logistics Analytics**: Delivery SLA tracking, carrier transit time distributions, state delay rankings, and review score impact curves.
6. **Seller Analytics**: Merchant lifetime distribution and empirical **Pareto 80/20** cumulative distribution curve.
7. **Data Pipeline Architecture**: Live database connection health check, Star Schema table record counter, technology stack breakdown, and Airflow orchestration audit log.

---

## 📁 Repository Structure

```
ecommerce-data-engineering-analytics-pipeline/
├── .env.example                     # Environment variables configuration template
├── .gitignore                        # Git exclusion rules (raw data, parquets, caches, secrets)
├── README.md                         # Comprehensive project documentation
├── requirements.txt                  # Python dependencies (PySpark, Streamlit, Plotly, SQLAlchemy)
├── config/
│   ├── __init__.py
│   └── config.yaml                   # Global configuration (paths, spark settings, thresholds)
├── dags/
│   ├── .gitkeep
│   └── ecommerce_data_pipeline.py    # 9-task Apache Airflow production DAG
├── dashboard/                        # Streamlit web application
│   ├── __init__.py
│   ├── app.py                        # Multi-page Streamlit main entrypoint
│   ├── components.py                 # Reusable UI widgets, cards, and Plotly charts
│   ├── database.py                   # SQLAlchemy connection pooling & query execution
│   └── queries.py                    # Parametric SQL query definitions for the dashboard
├── data/                             # Data directory (.gitkeep tracked, large files ignored)
│   ├── raw/                          # Raw CSV files
│   ├── processed/                    # Cleaned Parquet tables
│   └── analytics/                    # Pipeline validation & audit reports
├── docker/
│   ├── Dockerfile                    # Airflow image with Java 17, PySpark & dependencies
│   └── docker-compose.yaml           # Multi-container orchestration (Airflow + Postgres)
├── docs/
│   └── architecture.md               # Detailed architectural specification
├── scripts/
│   ├── check_dag_status.py           # Airflow DAG monitoring utility
│   ├── download_data.py              # Automated Kaggle/Olist dataset downloader
│   ├── test_dashboard_queries.py     # SQL query integration test
│   └── verify_dashboard_pages.py     # Streamlit page rendering verification
├── sql/
│   ├── ddl/
│   │   ├── create_staging_tables.sql # Staging raw tables DDL
│   │   └── create_star_schema.sql    # Kimball Star Schema DDL (`dwh` schema)
│   └── analytics/                    # Production SQL analytical queries (01 to 07)
│       ├── 01_monthly_revenue_growth.sql
│       ├── 02_top_product_categories.sql
│       ├── 03_delivery_performance.sql
│       ├── 04_customer_rfm_segmentation.sql
│       ├── 05_payment_type_distribution.sql
│       ├── 06_state_sales_performance.sql
│       └── 07_seller_performance.sql
├── src/                              # Core ETL & Warehouse codebase
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── ingest_raw.py             # File scanner, schema validation, raw profiling
│   ├── transformation/
│   │   ├── __init__.py
│   │   ├── build_star_schema.py      # Star schema transformation & surrogate keys
│   │   ├── clean_data.py             # PySpark data cleansing & normalization
│   │   └── spark_session.py          # PySpark Session singleton factory
│   ├── quality/
│   │   ├── __init__.py
│   │   └── data_quality_checks.py    # Automated 12-check PySpark quality gate
│   ├── warehouse/
│   │   ├── __init__.py
│   │   ├── postgres_loader.py        # High-performance batch streaming loader to Postgres
│   │   ├── run_analytics.py          # SQL analytics execution engine
│   │   └── validate_warehouse.py     # 25-check warehouse audit and reconciliation suite
│   └── utils/
│       ├── __init__.py
│       ├── db_connection.py          # Database URI and engine generator
│       └── logger.py                 # Formatted application logger
└── tests/
    ├── __init__.py
    └── test_data_quality.py          # PyTest test cases
```

---

## 🚀 Installation & Execution Guide

### 1. Prerequisites
- **Git**: [git-scm.com](https://git-scm.com/)
- **Python**: Version 3.10 or 3.11
- **Docker Desktop**: Running with Docker Compose support
- **Java**: OpenJDK 11 or 17 (required for local PySpark execution)

### 2. Clone Repository & Setup Virtual Environment
```bash
git clone https://github.com/tarunh2107/ecommerce-data-engineering-analytics-pipeline.git
cd ecommerce-data-engineering-analytics-pipeline

# Create Python virtual environment
python -m venv .venv

# Activate environment
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```bash
cp .env.example .env
# Update .env if your PostgreSQL or Airflow ports/passwords differ
```

### 4. Start PostgreSQL Warehouse Container
```bash
cd docker
docker-compose up -d postgres
cd ..
```

### 5. Download Raw Dataset
```bash
python scripts/download_data.py
```

### 6. Run Full ETL Pipeline Manually
```bash
# 1. Ingestion & Raw Validation
python src/ingestion/ingest_raw.py

# 2. PySpark Data Transformation
python src/transformation/clean_data.py

# 3. Quality Validation Gate
python src/quality/data_quality_checks.py

# 4. Star Schema Modeling
python src/transformation/build_star_schema.py

# 5. Load PostgreSQL Data Warehouse
python src/warehouse/postgres_loader.py

# 6. Run SQL Business Analytics
python src/warehouse/run_analytics.py

# 7. Run Warehouse Audit & Reconciliation
python src/warehouse/validate_warehouse.py
```

### 7. Run via Apache Airflow (Docker)
```bash
# Start full Airflow stack
cd docker
docker-compose up -d
cd ..

# Trigger the DAG via CLI or Airflow Web UI (http://localhost:8080)
docker exec airflow-scheduler airflow dags trigger ecommerce_data_pipeline
```

### 8. Launch Interactive Streamlit Website
```bash
streamlit run dashboard/app.py --server.port 8502
```
Navigate to `http://localhost:8502` in your browser.

---

## 📊 Production Verification & Reconciliation Results

All phases have undergone end-to-end automated reconciliation and verification:

| Metric / Check | Expected Target | Actual Verified Result | Status |
|---|---|---|---|
| **Raw Datasets Processed** | 9 files | 9 files | ✅ PASS |
| **Total Orders Ingested** | 99,441 | 99,441 | ✅ PASS |
| **Total Order Items Ingested** | 112,650 | 112,650 | ✅ PASS |
| **Warehouse Customers (`dim_customers`)** | 99,441 | 99,441 | ✅ PASS |
| **Warehouse Products (`dim_products`)** | 32,951 | 32,951 | ✅ PASS |
| **Warehouse Sellers (`dim_sellers`)** | 3,095 | 3,095 | ✅ PASS |
| **Warehouse Dates (`dim_date`)** | 1,826 | 1,826 | ✅ PASS |
| **Fact Orders (`fact_orders`)** | 99,441 | 99,441 | ✅ PASS |
| **Fact Order Items (`fact_order_items`)** | 112,650 | 112,650 | ✅ PASS |
| **Total Warehouse Rows** | 349,405 | 349,405 | ✅ PASS |
| **Primary Key Uniqueness** | 100.0% | 100.0% (0 duplicates) | ✅ PASS |
| **Foreign Key Orphan Rate** | 0.0% | 0.0% (0 orphans) | ✅ PASS |
| **Total Item Value Sum** | R$ 13,591,643.70 | R$ 13,591,643.70 | ✅ PASS |
| **Total Freight Value Sum** | R$ 1,985,352.84 | R$ 1,985,352.84 | ✅ PASS |
| **Total Gross Merchandise Value (GMV)**| R$ 15,422,461.53 | R$ 15,422,461.53 | ✅ PASS |
| **Financial Reconciliation Discrepancy**| **R$ 0.00** | **R$ 0.00 (0.00% drift)** | ✅ PASS |
| **Airflow DAG 9-Task Orchestration** | All Success | All Success | ✅ PASS |
| **Streamlit Multi-Page Verification** | 7/7 Pages Healthy | 7/7 Pages Healthy (Port 8502) | ✅ PASS |

---

## 🔮 Future Enhancements

- [ ] **Delta Lake / Apache Iceberg Integration**: Upgrade the processed storage layer to support ACID transactions, time-travel queries, and schema evolution.
- [ ] **dbt (data build tool) Integration**: Introduce dbt for SQL transformation modularization, documentation, and automated testing inside the warehouse layer.
- [ ] **Cloud Migration**: Deploy the containerized architecture to cloud-native platforms (AWS EMR/Redshift, Google Cloud Dataproc/BigQuery).
- [ ] **Real-Time Streaming**: Stream real-time clickstream or checkout events using Apache Kafka and Spark Structured Streaming.
- [ ] **CI/CD Automation**: Implement GitHub Actions workflows for automated linting (Black/Flake8), type checks (mypy), and integration testing with Docker services.

---

## 👤 Author & License

- **Author**: Tarun
- **GitHub**: [@tarunh2107](https://github.com/tarunh2107)
- **Repository**: [ecommerce-data-engineering-analytics-pipeline](https://github.com/tarunh2107/ecommerce-data-engineering-analytics-pipeline)
- **Code License**: This project codebase, transformations, SQL, and pipeline architecture are licensed under the **MIT License**.
- **Dataset Attribution**: The underlying e-commerce dataset is provided by **Olist** and hosted on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), used under the terms of the **CC BY-NC-SA 4.0** license.
