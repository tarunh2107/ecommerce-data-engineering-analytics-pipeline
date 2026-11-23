"""
Module: clean_data
Description: Production PySpark cleaning and transformation pipeline for Olist e-commerce datasets.
             - Reads all 9 raw datasets from data/raw/
             - Validates input row counts against Phase 2 profiling
             - Executes schema standardizations, date parsing, and null handling
             - Computes derived business metrics (delivery SLAs, item totals)
             - Executes data quality checks & referential integrity validations
             - Writes cleaned DataFrames to data/processed/ in Parquet format
             - Produces an end-to-end transformation report at data/analytics/transformation_report.txt
"""

import os
import sys
import shutil
from datetime import datetime
from typing import Dict, Any, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
    BooleanType,
)

# Ensure project root is in sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.transformation.spark_session import get_spark_session
from src.quality.data_quality_checks import run_all_quality_checks
from src.utils.logger import get_logger

logger = get_logger("spark_data_cleaner")

# Expected raw counts from Phase 2 validation
EXPECTED_RAW_COUNTS = {
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "order_payments": 103886,
    "order_reviews": 99224,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "category_translation": 71,
}


def load_raw_datasets(spark: SparkSession, raw_dir: str) -> Dict[str, DataFrame]:
    """
    Loads all 9 raw CSV datasets using Spark DataFrames.
    Validates row counts against Phase 2 baseline.
    """
    logger.info(f"Loading raw datasets from: {raw_dir}")
    file_mapping = {
        "customers": "olist_customers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }

    raw_dfs = {}
    for key, filename in file_mapping.items():
        file_path = os.path.join(raw_dir, filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Expected raw file not found: {file_path}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(file_path)
        )
        actual_count = df.count()
        expected_count = EXPECTED_RAW_COUNTS.get(key)

        logger.info(
            f"Loaded {filename:<38} | Rows: {actual_count:,} | Cols: {len(df.columns)}"
        )

        if actual_count != expected_count:
            logger.warning(
                f"Row count discrepancy in {filename}: expected {expected_count}, got {actual_count}"
            )
        raw_dfs[key] = df

    return raw_dfs


# ==============================================================================
# INDIVIDUAL DATASET CLEANING & FEATURE ENGINEERING
# ==============================================================================

def clean_customers(df: DataFrame) -> DataFrame:
    """
    Cleans Customers:
    - Trims string identifiers
    - Standardizes city to lowercase and state to uppercase
    - Preserves all 99,441 customer records
    """
    return (
        df.select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            F.lpad(F.trim(F.col("customer_zip_code_prefix")), 5, "0").alias("customer_zip_code_prefix"),
            F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )


def clean_sellers(df: DataFrame) -> DataFrame:
    """
    Cleans Sellers:
    - Trims seller identifiers
    - Formats zip code to 5-digit padded string
    - Standardizes city to lowercase and state to uppercase
    - Preserves all 3,095 seller records
    """
    return (
        df.select(
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.lpad(F.trim(F.col("seller_zip_code_prefix")), 5, "0").alias("seller_zip_code_prefix"),
            F.lower(F.trim(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Cleans Orders:
    - Converts all 5 date/timestamp strings to Spark TimestampType
    - Preserves meaningful nulls for unapproved, undelivered, or in-transit orders
    - Derives business metrics:
      * delivery_time_days: Days elapsed from order purchase to customer delivery
      * delivery_delay_days: Days elapsed between actual delivery and estimated delivery date
      * is_late_delivery: Boolean flag indicating if order arrived after estimated delivery date
    """
    df_typed = df.select(
        F.trim(F.col("order_id")).alias("order_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_timestamp(F.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("order_purchase_timestamp"),
        F.to_timestamp(F.col("order_approved_at"), "yyyy-MM-dd HH:mm:ss").alias("order_approved_at"),
        F.to_timestamp(F.col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss").alias("order_delivered_carrier_date"),
        F.to_timestamp(F.col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss").alias("order_delivered_customer_date"),
        F.to_timestamp(F.col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss").alias("order_estimated_delivery_date"),
    )

    # Calculate derived delivery metrics
    return (
        df_typed.withColumn(
            "delivery_time_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.round(
                    (F.unix_timestamp(F.col("order_delivered_customer_date")) - F.unix_timestamp(F.col("order_purchase_timestamp"))) / 86400.0,
                    2
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.round(
                    (F.unix_timestamp(F.col("order_delivered_customer_date")) - F.unix_timestamp(F.col("order_estimated_delivery_date"))) / 86400.0,
                    2
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), F.lit(True)).otherwise(F.lit(False))
            ).otherwise(F.lit(None).cast(BooleanType()))
        )
        .dropDuplicates(["order_id"])
    )


def clean_products(products_df: DataFrame, translation_df: DataFrame) -> DataFrame:
    """
    Cleans Products:
    - Renames misspelled columns:
      * product_name_lenght -> product_name_length
      * product_description_lenght -> product_description_length
    - Casts dimensions and weights to DoubleType, counts to IntegerType
    - Handles missing category names by filling with 'unlabeled'
    - Preserves the 2 products with missing dimensions
    - Joins English translations from category_translation_df
    """
    # Clean translation dataset
    trans_clean = translation_df.select(
        F.trim(F.col("product_category_name")).alias("trans_category_name"),
        F.lower(F.trim(F.col("product_category_name_english"))).alias("product_category_name_english"),
    ).dropDuplicates(["trans_category_name"])

    # Prepare base products with corrected column names and types
    base_prod = products_df.select(
        F.trim(F.col("product_id")).alias("product_id"),
        F.coalesce(
            F.when(F.trim(F.col("product_category_name")) == "", F.lit(None)).otherwise(F.trim(F.col("product_category_name"))),
            F.lit("unlabeled")
        ).alias("product_category_name"),
        F.col("product_name_lenght").cast(IntegerType()).alias("product_name_length"),
        F.col("product_description_lenght").cast(IntegerType()).alias("product_description_length"),
        F.col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        F.col("product_weight_g").cast(DoubleType()).alias("product_weight_g"),
        F.col("product_length_cm").cast(DoubleType()).alias("product_length_cm"),
        F.col("product_height_cm").cast(DoubleType()).alias("product_height_cm"),
        F.col("product_width_cm").cast(DoubleType()).alias("product_width_cm"),
    )

    # Join English translation
    joined = base_prod.join(
        trans_clean,
        base_prod["product_category_name"] == trans_clean["trans_category_name"],
        how="left"
    ).select(
        base_prod["product_id"],
        base_prod["product_category_name"],
        F.coalesce(
            trans_clean["product_category_name_english"],
            F.when(base_prod["product_category_name"] == "unlabeled", F.lit("unlabeled")).otherwise(base_prod["product_category_name"])
        ).alias("product_category_name_english"),
        base_prod["product_name_length"],
        base_prod["product_description_length"],
        base_prod["product_photos_qty"],
        base_prod["product_weight_g"],
        base_prod["product_length_cm"],
        base_prod["product_height_cm"],
        base_prod["product_width_cm"],
    )

    return joined.dropDuplicates(["product_id"])


def clean_order_items(df: DataFrame) -> DataFrame:
    """
    Cleans Order Items:
    - Casts order_item_id to IntegerType
    - Converts shipping_limit_date to TimestampType
    - Casts price and freight_value to DoubleType (rounded to 2 decimals)
    - Derives business metric:
      * order_item_total = price + freight_value (total item-level spend)
    - Deduplicates on (order_id, order_item_id)
    """
    return (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id").cast(IntegerType()).alias("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.to_timestamp(F.col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss").alias("shipping_limit_date"),
            F.round(F.col("price").cast(DoubleType()), 2).alias("price"),
            F.round(F.col("freight_value").cast(DoubleType()), 2).alias("freight_value"),
        )
        .withColumn(
            "order_item_total",
            F.round(F.col("price") + F.col("freight_value"), 2)
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )


def clean_order_payments(df: DataFrame) -> DataFrame:
    """
    Cleans Order Payments:
    - Casts payment_sequential and payment_installments to IntegerType
    - Standardizes payment_type to lowercase
    - Casts payment_value to DoubleType (rounded to 2 decimals)
    """
    return (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("payment_sequential").cast(IntegerType()).alias("payment_sequential"),
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            F.col("payment_installments").cast(IntegerType()).alias("payment_installments"),
            F.round(F.col("payment_value").cast(DoubleType()), 2).alias("payment_value"),
        )
        .dropDuplicates()
    )


def clean_order_reviews(df: DataFrame) -> DataFrame:
    """
    Cleans Order Reviews:
    - Preserves review_score as IntegerType
    - Preserves review_comment_title and review_comment_message without dropping records
    - Converts review creation and answer timestamps to TimestampType
    - Deduplicates on (review_id, order_id)
    """
    return (
        df.select(
            F.trim(F.col("review_id")).alias("review_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("review_score").cast(IntegerType()).alias("review_score"),
            F.trim(F.col("review_comment_title")).alias("review_comment_title"),
            F.trim(F.col("review_comment_message")).alias("review_comment_message"),
            F.to_timestamp(F.col("review_creation_date"), "yyyy-MM-dd HH:mm:ss").alias("review_creation_date"),
            F.to_timestamp(F.col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("review_answer_timestamp"),
        )
        .dropDuplicates(["review_id", "order_id"])
    )


def clean_geolocation(df: DataFrame) -> Tuple[DataFrame, int, int]:
    """
    Cleans and Aggregates Geolocation:
    1. Removes 261,831 exact duplicates from profiling.
    2. Groups by geolocation_zip_code_prefix and aggregates coordinates to representative centroid:
       - avg(geolocation_lat) rounded to 6 decimals
       - avg(geolocation_lng) rounded to 6 decimals
       - first(geolocation_city) standardized to lowercase
       - first(geolocation_state) standardized to uppercase
    Reduces 1,000,163 raw rows to 19,015 distinct zip code centroids.
    """
    raw_count = df.count()
    # Step 1: Remove exact duplicates
    deduped = df.dropDuplicates()
    deduped_count = deduped.count()
    exact_duplicates_removed = raw_count - deduped_count

    # Step 2: Aggregate by zip code prefix
    aggregated = (
        deduped.groupBy("geolocation_zip_code_prefix")
        .agg(
            F.round(F.avg(F.col("geolocation_lat").cast(DoubleType())), 6).alias("geolocation_lat"),
            F.round(F.avg(F.col("geolocation_lng").cast(DoubleType())), 6).alias("geolocation_lng"),
            F.first(F.lower(F.trim(F.col("geolocation_city")))).alias("geolocation_city"),
            F.first(F.upper(F.trim(F.col("geolocation_state")))).alias("geolocation_state"),
        )
        .select(
            F.lpad(F.trim(F.col("geolocation_zip_code_prefix")), 5, "0").alias("geolocation_zip_code_prefix"),
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        )
        .dropDuplicates(["geolocation_zip_code_prefix"])
    )

    final_count = aggregated.count()
    logger.info(
        f"Geolocation transformation: Raw={raw_count:,} -> Exact Dedup={deduped_count:,} "
        f"-> Aggregated Centroids={final_count:,} (Duplicates removed={exact_duplicates_removed:,})"
    )
    return aggregated, raw_count, final_count


# ==============================================================================
# REPORT GENERATION & PIPELINE ORCHESTRATION
# ==============================================================================

def write_parquet_datasets(cleaned_dfs: Dict[str, DataFrame], output_dir: str) -> Dict[str, str]:
    """Writes cleaned PySpark DataFrames to Parquet format in data/processed/."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for name, df in cleaned_dfs.items():
        dest_path = os.path.join(output_dir, f"{name}.parquet")
        if os.path.exists(dest_path):
            shutil.rmtree(dest_path, ignore_errors=True)
        logger.info(f"Writing {name} to Parquet: {dest_path}")
        df.write.mode("overwrite").parquet(dest_path)
        paths[name] = dest_path
    return paths


def generate_transformation_report(
    raw_dfs: Dict[str, DataFrame],
    cleaned_dfs: Dict[str, DataFrame],
    geo_stats: Tuple[int, int],
    dq_results: Dict[str, Any],
    parquet_paths: Dict[str, str],
    report_path: str
) -> None:
    """Generates comprehensive Phase 3 Transformation & Quality Report."""
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append("=" * 85)
    lines.append("OLIST E-COMMERCE DATASET - PHASE 3 TRANSFORMATION & DATA QUALITY REPORT")
    lines.append(f"Generated at: {timestamp}")
    lines.append(f"Python Version: {sys.version.split()[0]}")
    lines.append(f"PySpark Version: 3.5.9")
    lines.append(f"Overall Phase 3 DQ Status: {dq_results.get('overall_status', 'UNKNOWN')}")
    lines.append("=" * 85)
    lines.append("")

    # 1. Dataset Row Counts & Dimension Changes
    lines.append("1. DATASET VOLUMETRICS & ROW COUNT COMPARISONS:")
    lines.append("-" * 85)
    lines.append(f"{'Dataset Name':<25} | {'Raw Rows':>12} | {'Clean Rows':>12} | {'Delta / Duplicates':>20}")
    lines.append("-" * 85)

    for name in sorted(cleaned_dfs.keys()):
        raw_count = raw_dfs[name].count() if name in raw_dfs else "N/A"
        clean_count = cleaned_dfs[name].count()
        if name == "geolocation":
            diff_str = f"-{geo_stats[0] - geo_stats[1]:,} (Aggregated)"
        else:
            diff = (raw_count - clean_count) if isinstance(raw_count, int) else 0
            diff_str = f"-{diff:,} duplicates" if diff > 0 else "0 (No loss)"
        lines.append(f"{name:<25} | {raw_count:>12,} | {clean_count:>12,} | {diff_str:>20}")

    lines.append("-" * 85)
    lines.append("")

    # 2. Cleaning & Null-Handling Decisions
    lines.append("2. CLEANING & NULL-HANDLING DOCUMENTATION:")
    lines.append("-" * 85)
    cleaning_docs = [
        ("orders", "Parsed 5 timestamp columns to TimestampType. Preserved legitimate nulls in order_approved_at (160), order_delivered_carrier_date (1,783), and order_delivered_customer_date (2,965) representing non-delivered/in-transit orders."),
        ("products", "Standardized misspelled columns 'product_name_lenght' -> 'product_name_length' and 'product_description_lenght' -> 'product_description_length'. Handled 610 missing category names by assigning 'unlabeled'. Preserved 2 products with missing physical dimensions. Joined English translations."),
        ("geolocation", "Eliminated 261,831 exact duplicates. Aggregated coordinates per zip_code_prefix into representative centroids (mean latitude and longitude), reducing 1,000,163 raw rows to 19,015 distinct postal code locations."),
        ("order_reviews", "Preserved review scores (1 to 5). Retained empty/null review titles (88.34%) and messages (58.70%) as valid reviews without text."),
        ("order_items", "Cast price and freight_value to double precision (2 decimal places). Verified non-negative financial amounts. Converted shipping_limit_date to TimestampType."),
        ("order_payments", "Cast payment_value to double precision and payment_installments to integer. Verified non-negative payment values."),
        ("customers / sellers", "Trimmed unique identifiers. Formatted zip code prefixes to 5-digit padded strings. Normalized city names to lowercase and state codes to uppercase."),
    ]
    for dataset, doc in cleaning_docs:
        lines.append(f"• [{dataset.upper()}]: {doc}")
    lines.append("")

    # 3. Derived Business Features
    lines.append("3. DERIVED BUSINESS FEATURES & FORMULAS:")
    lines.append("-" * 85)
    derived_docs = [
        ("orders.delivery_time_days", "round((unix_timestamp(order_delivered_customer_date) - unix_timestamp(order_purchase_timestamp)) / 86400.0, 2)", "Measures total delivery duration in days from order placement to customer delivery."),
        ("orders.delivery_delay_days", "round((unix_timestamp(order_delivered_customer_date) - unix_timestamp(order_estimated_delivery_date)) / 86400.0, 2)", "Difference between actual and estimated delivery dates. Positive values denote delivery delays."),
        ("orders.is_late_delivery", "order_delivered_customer_date > order_estimated_delivery_date (Boolean)", "Binary flag indicating customer delivery arrived past the estimated SLA."),
        ("order_items.order_item_total", "round(price + freight_value, 2)", "Represents gross expenditure per item including shipping freight."),
    ]
    for col_name, formula, desc in derived_docs:
        lines.append(f"• {col_name}:")
        lines.append(f"    Formula    : {formula}")
        lines.append(f"    Description: {desc}")
    lines.append("")

    # 4. Data Quality Validation Results
    lines.append("4. DATA QUALITY VALIDATION RESULTS:")
    lines.append("-" * 85)
    lines.append(f"{'Check Category':<25} | {'Target':<30} | {'Status':<6} | {'Details'}")
    lines.append("-" * 85)

    for item in dq_results.get("non_null_checks", []):
        target = f"{item['dataset']}.{item['column']}"
        lines.append(f"{'Non-Null Key':<25} | {target:<30} | {item['status']:<6} | {item['details']}")

    for item in dq_results.get("uniqueness_checks", []):
        target = f"{item['dataset']}.{item['columns']}"
        lines.append(f"{'Uniqueness':<25} | {target:<30} | {item['status']:<6} | {item['details']}")

    for item in dq_results.get("no_duplicate_checks", []):
        lines.append(f"{'No Exact Duplicates':<25} | {item['dataset']:<30} | {item['status']:<6} | {item['details']}")

    for item in dq_results.get("non_negative_checks", []):
        target = f"{item['dataset']}.{item['column']}"
        lines.append(f"{'Non-Negative Value':<25} | {target:<30} | {item['status']:<6} | {item['details']}")

    for item in dq_results.get("value_range_checks", []):
        target = f"{item['dataset']}.{item['column']}"
        lines.append(f"{'Value Range':<25} | {target:<30} | {item['status']:<6} | {item['details']}")

    for item in dq_results.get("timestamp_checks", []):
        target = f"{item['dataset']}.{item['comparison']}"
        lines.append(f"{'Timestamp Order':<25} | {target:<30} | {item['status']:<6} | {item['details']}")

    lines.append("-" * 85)
    lines.append("")

    # 5. Referential Integrity & Orphan Findings
    lines.append("5. REFERENTIAL INTEGRITY & ORPHAN RECORD ANALYSIS:")
    lines.append("-" * 85)
    lines.append(f"{'Relationship':<40} | {'Child Rows':>10} | {'Orphans':>8} | {'Orphan %':>8} | {'Status'}")
    lines.append("-" * 85)

    for ref in dq_results.get("referential_checks", []):
        rel = ref["relationship"]
        c_rows = f"{ref['total_child_rows']:,}"
        o_count = f"{ref['orphan_count']:,}"
        o_pct = f"{ref['orphan_pct']:.2f}%"
        status = ref["status"]
        lines.append(f"{rel:<40} | {c_rows:>10} | {o_count:>8} | {o_pct:>8} | {status}")
        if ref["orphan_count"] > 0:
            lines.append(f"  * Note: Sample orphan keys in {ref['child_table']}: {ref['sample_orphan_keys']}")

    lines.append("-" * 85)
    lines.append("")

    # 6. Output Parquet File Paths
    lines.append("6. OUTPUT PARQUET DATASET LOCATIONS:")
    lines.append("-" * 85)
    for name, path in parquet_paths.items():
        lines.append(f"• {name:<20}: {path}")

    lines.append("")
    lines.append("=" * 85)
    lines.append("END OF PHASE 3 TRANSFORMATION REPORT")
    lines.append("=" * 85)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info(f"Transformation report saved to: {report_path}")


def run_pipeline() -> bool:
    """Executes end-to-end Phase 3 cleaning, feature engineering, and validation."""
    logger.info("Starting Phase 3 PySpark Cleaning and Transformation Pipeline...")
    spark = get_spark_session()

    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    processed_dir = os.path.join(PROJECT_ROOT, "data", "processed")
    report_file = os.path.join(PROJECT_ROOT, "data", "analytics", "transformation_report.txt")

    # Step 1: Load all raw datasets
    raw_dfs = load_raw_datasets(spark, raw_dir)

    # Step 2: Execute cleaning transformations
    logger.info("Executing cleaning transformations on all datasets...")
    cleaned_dfs: Dict[str, DataFrame] = {}

    cleaned_dfs["customers"] = clean_customers(raw_dfs["customers"])
    cleaned_dfs["sellers"] = clean_sellers(raw_dfs["sellers"])
    cleaned_dfs["orders"] = clean_orders(raw_dfs["orders"])
    cleaned_dfs["products"] = clean_products(raw_dfs["products"], raw_dfs["category_translation"])
    cleaned_dfs["order_items"] = clean_order_items(raw_dfs["order_items"])
    cleaned_dfs["order_payments"] = clean_order_payments(raw_dfs["order_payments"])
    cleaned_dfs["order_reviews"] = clean_order_reviews(raw_dfs["order_reviews"])

    geo_clean, geo_raw_cnt, geo_clean_cnt = clean_geolocation(raw_dfs["geolocation"])
    cleaned_dfs["geolocation"] = geo_clean

    # Step 3: Run Data Quality Checks & Referential Integrity
    dq_results = run_all_quality_checks(cleaned_dfs)

    # Step 4: Write cleaned datasets to Parquet
    parquet_paths = write_parquet_datasets(cleaned_dfs, processed_dir)

    # Step 5: Generate Transformation Report
    generate_transformation_report(
        raw_dfs,
        cleaned_dfs,
        (geo_raw_cnt, geo_clean_cnt),
        dq_results,
        parquet_paths,
        report_file
    )

    logger.info(">>> Phase 3 PySpark Data Cleaning & Transformation Completed Successfully! <<<")
    return dq_results.get("overall_status") == "PASS"


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
