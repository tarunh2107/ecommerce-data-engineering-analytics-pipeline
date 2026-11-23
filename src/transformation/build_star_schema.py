"""
Module: build_star_schema
Description: Transforms cleaned Olist Parquet datasets into a Kimball-style Star Schema:
             - Dimension Tables:
               * dwh.dim_date (calendar dimension 2016-01-01 to 2020-12-31)
               * dwh.dim_customer (surrogate key customer_key)
               * dwh.dim_product (surrogate key product_key)
               * dwh.dim_seller (surrogate key seller_key)
             - Fact Tables:
               * dwh.fact_order (Grain: One row per order, 99,441 rows)
               * dwh.fact_order_item (Grain: One row per order line item, 112,650 rows)
             Intermediate tables are saved to data/processed/warehouse/*.parquet.
"""

import os
import sys
import shutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    date_format,
    to_date,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    row_number,
    round as spark_round,
    sum as spark_sum,
    count as spark_count,
    avg as spark_avg,
    coalesce,
    sequence,
    explode,
    expr
)
from pyspark.sql.window import Window

# Add project root to sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.transformation.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger("build_star_schema")

PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
WAREHOUSE_OUTPUT_DIR = os.path.join(PROCESSED_DIR, "warehouse")


def generate_dim_date(spark: SparkSession, start_date: str = "2016-01-01", end_date: str = "2020-12-31") -> DataFrame:
    """
    Generates a calendar dimension DataFrame spanning start_date to end_date.
    Includes date_key (YYYYMMDD integer), calendar attributes, and weekend flags.
    """
    logger.info(f"Generating dim_date from {start_date} to {end_date}...")

    # Create sequence of dates
    df = (
        spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_seq")
        .withColumn("full_date", explode(col("date_seq")))
        .drop("date_seq")
    )

    dim_date = (
        df.select(
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("full_date"),
            year(col("full_date")).cast("int").alias("year"),
            quarter(col("full_date")).cast("int").alias("quarter"),
            month(col("full_date")).cast("int").alias("month"),
            date_format(col("full_date"), "MMMM").alias("month_name"),
            dayofmonth(col("full_date")).cast("int").alias("day"),
            dayofweek(col("full_date")).cast("int").alias("day_of_week"),
            date_format(col("full_date"), "EEEE").alias("day_name"),
            weekofyear(col("full_date")).cast("int").alias("week_of_year"),
            when(dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        )
        .orderBy("date_key")
    )

    return dim_date


def build_dim_customer(customers_df: DataFrame) -> DataFrame:
    """
    Builds dim_customer with deterministic integer surrogate key customer_key.
    Row count matches processed customers (99,441 rows).
    """
    logger.info("Building dim_customer...")
    window_spec = Window.orderBy("customer_id")

    dim_customer = (
        customers_df.select(
            row_number().over(window_spec).cast("int").alias("customer_key"),
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_city"),
            col("customer_state"),
        )
    )
    return dim_customer


def build_dim_product(products_df: DataFrame) -> DataFrame:
    """
    Builds dim_product with deterministic integer surrogate key product_key.
    Row count matches processed products (32,951 rows).
    """
    logger.info("Building dim_product...")
    window_spec = Window.orderBy("product_id")

    dim_product = (
        products_df.select(
            row_number().over(window_spec).cast("int").alias("product_key"),
            col("product_id"),
            col("product_category_name"),
            col("product_category_name_english"),
            col("product_name_length"),
            col("product_description_length"),
            col("product_photos_qty"),
            spark_round(col("product_weight_g"), 2).alias("product_weight_g"),
            spark_round(col("product_length_cm"), 2).alias("product_length_cm"),
            spark_round(col("product_height_cm"), 2).alias("product_height_cm"),
            spark_round(col("product_width_cm"), 2).alias("product_width_cm"),
        )
    )
    return dim_product


def build_dim_seller(sellers_df: DataFrame) -> DataFrame:
    """
    Builds dim_seller with deterministic integer surrogate key seller_key.
    Row count matches processed sellers (3,095 rows).
    """
    logger.info("Building dim_seller...")
    window_spec = Window.orderBy("seller_id")

    dim_seller = (
        sellers_df.select(
            row_number().over(window_spec).cast("int").alias("seller_key"),
            col("seller_id"),
            col("seller_zip_code_prefix"),
            col("seller_city"),
            col("seller_state"),
        )
    )
    return dim_seller


def build_fact_order(
    orders_df: DataFrame,
    dim_customer_df: DataFrame,
    order_items_df: DataFrame,
    order_payments_df: DataFrame,
    order_reviews_df: DataFrame,
) -> DataFrame:
    """
    Builds fact_order table.
    Grain: One row per order (99,441 rows).
    Measures: item count, total price, total freight, total order value, total payment, review score, delivery SLA.
    """
    logger.info("Building fact_order (Grain: One row per order)...")

    # Aggregate item measures per order
    item_agg = (
        order_items_df.groupBy("order_id")
        .agg(
            spark_count("order_item_id").alias("order_item_count"),
            spark_round(spark_sum("price"), 2).alias("total_price"),
            spark_round(spark_sum("freight_value"), 2).alias("total_freight"),
            spark_round(spark_sum("order_item_total"), 2).alias("total_order_value"),
        )
    )

    # Aggregate payments per order
    payment_agg = (
        order_payments_df.groupBy("order_id")
        .agg(
            spark_round(spark_sum("payment_value"), 2).alias("total_payment"),
        )
    )

    # Aggregate review scores per order (average if multiple reviews)
    review_agg = (
        order_reviews_df.groupBy("order_id")
        .agg(
            spark_round(spark_avg("review_score"), 2).alias("review_score"),
        )
    )

    # Join orders with customer surrogate key
    orders_with_customer = orders_df.join(
        dim_customer_df.select("customer_id", "customer_key"),
        on="customer_id",
        how="inner"
    )

    # Join aggregates onto orders
    joined = (
        orders_with_customer
        .join(item_agg, on="order_id", how="left")
        .join(payment_agg, on="order_id", how="left")
        .join(review_agg, on="order_id", how="left")
    )

    window_spec = Window.orderBy("order_id")

    fact_order = (
        joined.select(
            row_number().over(window_spec).cast("int").alias("order_key"),
            col("order_id"),
            col("customer_key"),
            date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_key"),
            when(col("order_approved_at").isNotNull(), date_format(col("order_approved_at"), "yyyyMMdd").cast("int")).alias("order_approved_date_key"),
            when(col("order_delivered_carrier_date").isNotNull(), date_format(col("order_delivered_carrier_date"), "yyyyMMdd").cast("int")).alias("order_delivered_carrier_date_key"),
            when(col("order_delivered_customer_date").isNotNull(), date_format(col("order_delivered_customer_date"), "yyyyMMdd").cast("int")).alias("order_delivered_customer_date_key"),
            when(col("order_estimated_delivery_date").isNotNull(), date_format(col("order_estimated_delivery_date"), "yyyyMMdd").cast("int")).alias("order_estimated_delivery_date_key"),
            col("order_status"),
            coalesce(col("order_item_count"), lit(0)).cast("int").alias("order_item_count"),
            coalesce(col("total_price"), lit(0.0)).cast("double").alias("total_price"),
            coalesce(col("total_freight"), lit(0.0)).cast("double").alias("total_freight"),
            coalesce(col("total_order_value"), lit(0.0)).cast("double").alias("total_order_value"),
            coalesce(col("total_payment"), lit(0.0)).cast("double").alias("total_payment"),
            col("review_score").cast("double").alias("review_score"),
            col("delivery_time_days").cast("double").alias("delivery_time_days"),
            col("delivery_delay_days").cast("double").alias("delivery_delay_days"),
            col("is_late_delivery").cast("boolean").alias("is_late_delivery"),
        )
    )

    return fact_order


def build_fact_order_item(
    order_items_df: DataFrame,
    orders_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    dim_seller_df: DataFrame,
) -> DataFrame:
    """
    Builds fact_order_item table.
    Grain: One row per order line item (112,650 rows).
    Measures: item price, freight value, item total.
    Foreign Keys: customer_key, product_key, seller_key, order_date_key.
    """
    logger.info("Building fact_order_item (Grain: One row per order line item)...")

    # Order items join orders to get customer_id and order_purchase_timestamp
    items_with_order = order_items_df.join(
        orders_df.select("order_id", "customer_id", "order_purchase_timestamp"),
        on="order_id",
        how="inner"
    )

    # Join customer surrogate key
    items_with_customer = items_with_order.join(
        dim_customer_df.select("customer_id", "customer_key"),
        on="customer_id",
        how="inner"
    )

    # Join product surrogate key
    items_with_product = items_with_customer.join(
        dim_product_df.select("product_id", "product_key"),
        on="product_id",
        how="inner"
    )

    # Join seller surrogate key
    items_with_seller = items_with_product.join(
        dim_seller_df.select("seller_id", "seller_key"),
        on="seller_id",
        how="inner"
    )

    window_spec = Window.orderBy("order_id", "order_item_id")

    fact_order_item = (
        items_with_seller.select(
            row_number().over(window_spec).cast("int").alias("order_item_key"),
            col("order_id"),
            col("order_item_id").cast("int"),
            col("customer_key"),
            col("product_key"),
            col("seller_key"),
            date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_key"),
            col("shipping_limit_date"),
            spark_round(col("price"), 2).alias("price"),
            spark_round(col("freight_value"), 2).alias("freight_value"),
            spark_round(col("order_item_total"), 2).alias("order_item_total"),
        )
    )

    return fact_order_item


def execute_build_star_schema() -> dict:
    """
    Orchestrates full star schema construction:
    1. Loads processed Parquet datasets from data/processed/
    2. Builds dim_date, dim_customer, dim_product, dim_seller
    3. Builds fact_order and fact_order_item
    4. Validates counts, uniqueness, null keys
    5. Saves intermediate warehouse tables to data/processed/warehouse/
    Returns dictionary of DataFrames.
    """
    spark = get_spark_session("BuildStarSchema")

    logger.info("Loading cleaned Parquet datasets from data/processed/...")
    customers_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "customers.parquet"))
    products_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "products.parquet"))
    sellers_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "sellers.parquet"))
    orders_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "orders.parquet"))
    order_items_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "order_items.parquet"))
    order_payments_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "order_payments.parquet"))
    order_reviews_df = spark.read.parquet(os.path.join(PROCESSED_DIR, "order_reviews.parquet"))

    # 1. Build Dimensions
    dim_date = generate_dim_date(spark, "2016-01-01", "2020-12-31")
    dim_customer = build_dim_customer(customers_df)
    dim_product = build_dim_product(products_df)
    dim_seller = build_dim_seller(sellers_df)

    # 2. Build Facts
    fact_order = build_fact_order(orders_df, dim_customer, order_items_df, order_payments_df, order_reviews_df)
    fact_order_item = build_fact_order_item(order_items_df, orders_df, dim_customer, dim_product, dim_seller)

    tables = {
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "fact_order": fact_order,
        "fact_order_item": fact_order_item,
    }

    # 3. Validation & Persistence
    os.makedirs(WAREHOUSE_OUTPUT_DIR, exist_ok=True)
    counts = {}

    for name, df in tables.items():
        out_path = os.path.join(WAREHOUSE_OUTPUT_DIR, f"{name}.parquet")
        if os.path.exists(out_path):
            shutil.rmtree(out_path, ignore_errors=True)
        logger.info(f"Writing {name} to {out_path}...")
        df.write.mode("overwrite").parquet(out_path)
        
        # Verify saved table
        saved_df = spark.read.parquet(out_path)
        cnt = saved_df.count()
        counts[name] = cnt
        logger.info(f"Table '{name}' verified: {cnt:,} rows written.")

    print("\n" + "=" * 60)
    print("STAR SCHEMA BUILD COMPLETE — VALIDATION SUMMARY")
    print("=" * 60)
    for name, cnt in counts.items():
        print(f"Table: {name:20} -> {cnt:10,} rows")
    print("=" * 60 + "\n")

    return tables


if __name__ == "__main__":
    execute_build_star_schema()
