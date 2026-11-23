"""
Module: postgres_loader
Description: High-performance batch loading of Kimball Star Schema dimensional
             tables from Parquet into PostgreSQL data warehouse (olist_dw).
             Enforces dependency order, validates referential integrity, and
             supports idempotent reloads via controlled TRUNCATE ... CASCADE.
"""

import io
import os
import sys
import time
from typing import Dict, List, Optional
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from dotenv import load_dotenv

# Ensure project root is in sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("postgres_loader")

# Load environment variables from .env
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path=ENV_PATH)

WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "warehouse")
DDL_FILE_PATH = os.path.join(PROJECT_ROOT, "sql", "ddl", "create_star_schema.sql")


def get_db_connection() -> PgConnection:
    """
    Establishes and returns a psycopg2 connection using environment variables.
    """
    dbname = os.getenv("POSTGRES_DB", "olist_dw")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", 5432))

    logger.info(f"Connecting to PostgreSQL database '{dbname}' at {host}:{port} as user '{user}'...")
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    return conn


def init_star_schema_ddl(conn: PgConnection, ddl_path: str = DDL_FILE_PATH) -> None:
    """
    Executes the DDL script to create dwh schema, tables, views, and indexes.
    """
    logger.info(f"Applying Star Schema DDL from {ddl_path}...")
    if not os.path.exists(ddl_path):
        raise FileNotFoundError(f"DDL file not found at: {ddl_path}")

    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(ddl_sql)
    conn.commit()
    logger.info("Star Schema DDL applied successfully.")


def truncate_star_schema(conn: PgConnection) -> None:
    """
    Performs controlled TRUNCATE CASCADE on all warehouse tables to guarantee idempotency.
    """
    logger.info("Truncating warehouse tables for idempotent reload...")
    truncate_sql = """
        TRUNCATE TABLE 
            dwh.fact_order_item,
            dwh.fact_order,
            dwh.dim_seller,
            dwh.dim_product,
            dwh.dim_customer,
            dwh.dim_date
        CASCADE;
    """
    with conn.cursor() as cur:
        cur.execute(truncate_sql)
    conn.commit()
    logger.info("Warehouse tables truncated successfully.")


def load_parquet_table(conn: PgConnection, table_name: str, schema_name: str = "dwh") -> int:
    """
    Loads a single Parquet file into PostgreSQL using high-speed streaming COPY.
    Converts null values cleanly and handles boolean/numeric representations.
    """
    parquet_path = os.path.join(WAREHOUSE_DIR, f"{table_name}.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet dataset not found: {parquet_path}")

    logger.info(f"Reading {parquet_path} into memory...")
    df = pd.read_parquet(parquet_path)
    row_count = len(df)
    columns = list(df.columns)
    col_names = ", ".join(f'"{col}"' for col in columns)

    # Integer columns mapping to ensure clean integer text without decimal '.0'
    integer_columns_map = {
        "dim_date": ["date_key", "year", "quarter", "month", "day", "day_of_week", "week_of_year"],
        "dim_customer": ["customer_key"],
        "dim_product": ["product_key", "product_name_length", "product_description_length", "product_photos_qty"],
        "dim_seller": ["seller_key"],
        "fact_order": [
            "order_key", "customer_key", "order_date_key", "order_approved_date_key",
            "order_delivered_carrier_date_key", "order_delivered_customer_date_key",
            "order_estimated_delivery_date_key", "order_item_count"
        ],
        "fact_order_item": [
            "order_item_key", "order_item_id", "customer_key", "product_key",
            "seller_key", "order_date_key"
        ],
    }

    if table_name in integer_columns_map:
        for int_col in integer_columns_map[table_name]:
            if int_col in df.columns:
                df[int_col] = df[int_col].astype("Int64")

    logger.info(f"Streaming {row_count:,} rows into {schema_name}.{table_name} via COPY...")
    start_time = time.time()

    # Convert DataFrame to in-memory CSV buffer with '\\N' for NULL
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N", sep="\t")
    buffer.seek(0)

    copy_sql = f"COPY {schema_name}.{table_name} ({col_names}) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')"

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buffer)
    conn.commit()

    duration = time.time() - start_time
    logger.info(f"Loaded {schema_name}.{table_name}: {row_count:,} rows in {duration:.2f}s ({row_count / max(duration, 0.001):,.0f} rows/s).")
    return row_count


def load_star_schema(recreate_ddl: bool = True) -> Dict[str, int]:
    """
    Orchestrates warehouse loading respecting strict Kimball dependency order:
    1. dim_date
    2. dim_customer
    3. dim_product
    4. dim_seller
    5. fact_order
    6. fact_order_item
    """
    conn = get_db_connection()
    load_results = {}

    try:
        if recreate_ddl:
            init_star_schema_ddl(conn)
        else:
            truncate_star_schema(conn)

        # Loading order: Dimensions first, then facts
        loading_sequence = [
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_seller",
            "fact_order",
            "fact_order_item",
        ]

        logger.info(f"Starting star schema load sequence: {' -> '.join(loading_sequence)}")
        total_start = time.time()

        for tbl in loading_sequence:
            count = load_parquet_table(conn, tbl)
            load_results[tbl] = count

        total_duration = time.time() - total_start
        total_rows = sum(load_results.values())
        logger.info(f"Completed star schema load: {total_rows:,} total rows loaded in {total_duration:.2f}s.")

        print("\n" + "=" * 60)
        print("POSTGRESQL STAR SCHEMA LOAD SUMMARY")
        print("=" * 60)
        for tbl, count in load_results.items():
            print(f"Table: dwh.{tbl:18} -> {count:10,} rows loaded")
        print("=" * 60)
        print(f"Total rows loaded: {total_rows:,} in {total_duration:.2f} seconds.")
        print("=" * 60 + "\n")

    finally:
        conn.close()

    return load_results


if __name__ == "__main__":
    load_star_schema(recreate_ddl=True)
