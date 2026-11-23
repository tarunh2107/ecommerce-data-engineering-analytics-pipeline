"""
Module: data_quality_checks
Description: Data quality framework for PySpark DataFrames.
             Performs non-null assertions on keys, uniqueness checks, duplicate detection,
             range and non-negativity checks, timestamp order validation, and referential
             integrity checks (identifying orphan records without deleting them).
"""

from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger("data_quality")


def check_non_null_keys(df: DataFrame, dataset_name: str, key_columns: List[str]) -> List[Dict[str, Any]]:
    """Checks that primary or business key columns contain zero null values."""
    results = []
    total_rows = df.count()
    for col_name in key_columns:
        if col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull() | F.isnan(F.col(col_name))).count()
            passed = (null_count == 0)
            results.append({
                "check_type": "NON_NULL_KEY",
                "dataset": dataset_name,
                "column": col_name,
                "total_rows": total_rows,
                "failed_count": null_count,
                "failed_pct": round((null_count / total_rows * 100) if total_rows > 0 else 0.0, 4),
                "status": "PASS" if passed else "FAIL",
                "details": f"Zero nulls expected for {col_name}, found {null_count}"
            })
            if not passed:
                logger.error(f"[DQ FAIL] {dataset_name}.{col_name} has {null_count} null keys!")
        else:
            results.append({
                "check_type": "NON_NULL_KEY",
                "dataset": dataset_name,
                "column": col_name,
                "total_rows": total_rows,
                "failed_count": total_rows,
                "failed_pct": 100.0,
                "status": "FAIL",
                "details": f"Column {col_name} missing from dataset {dataset_name}"
            })
    return results


def check_uniqueness(df: DataFrame, dataset_name: str, key_columns: List[str]) -> Dict[str, Any]:
    """Checks that the specified key columns uniquely identify each row in the dataset."""
    total_rows = df.count()
    distinct_count = df.select(key_columns).distinct().count()
    duplicates = total_rows - distinct_count
    passed = (duplicates == 0)

    result = {
        "check_type": "UNIQUENESS",
        "dataset": dataset_name,
        "columns": key_columns,
        "total_rows": total_rows,
        "distinct_keys": distinct_count,
        "duplicate_keys": duplicates,
        "status": "PASS" if passed else "FAIL",
        "details": f"Expected unique {key_columns}. Duplicate keys: {duplicates}"
    }
    if not passed:
        logger.warning(f"[DQ WARN] {dataset_name} has {duplicates} duplicate entries for key {key_columns}")
    return result


def check_no_exact_duplicates(df: DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Verifies that the DataFrame contains 0 exact duplicate rows across all columns."""
    total_rows = df.count()
    distinct_rows = df.dropDuplicates().count()
    duplicate_rows = total_rows - distinct_rows
    passed = (duplicate_rows == 0)

    result = {
        "check_type": "NO_EXACT_DUPLICATES",
        "dataset": dataset_name,
        "total_rows": total_rows,
        "duplicate_rows": duplicate_rows,
        "status": "PASS" if passed else "FAIL",
        "details": f"Expected 0 exact duplicates, found {duplicate_rows}"
    }
    return result


def check_non_negative_values(df: DataFrame, dataset_name: str, numeric_columns: List[str]) -> List[Dict[str, Any]]:
    """Asserts that financial and quantity metrics are non-negative (>= 0)."""
    results = []
    total_rows = df.count()
    for col_name in numeric_columns:
        if col_name in df.columns:
            neg_count = df.filter(F.col(col_name) < 0).count()
            passed = (neg_count == 0)
            results.append({
                "check_type": "NON_NEGATIVE_VALUE",
                "dataset": dataset_name,
                "column": col_name,
                "total_rows": total_rows,
                "failed_count": neg_count,
                "status": "PASS" if passed else "FAIL",
                "details": f"Expected non-negative values in {col_name}. Found {neg_count} negative entries."
            })
    return results


def check_value_range(df: DataFrame, dataset_name: str, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
    """Validates that a numeric column falls within an acceptable domain range [min_val, max_val]."""
    total_rows = df.count()
    out_of_range = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()
    passed = (out_of_range == 0)
    return {
        "check_type": "VALUE_RANGE",
        "dataset": dataset_name,
        "column": column,
        "expected_range": f"[{min_val}, {max_val}]",
        "total_rows": total_rows,
        "failed_count": out_of_range,
        "status": "PASS" if passed else "FAIL",
        "details": f"{column} entries outside range [{min_val}, {max_val}]: {out_of_range}"
    }


def check_timestamp_order(df: DataFrame, dataset_name: str, earlier_col: str, later_col: str) -> Dict[str, Any]:
    """Validates that chronologically earlier events occur before or on later events."""
    # Filter only rows where both timestamps are not null
    subset = df.filter(F.col(earlier_col).isNotNull() & F.col(later_col).isNotNull())
    total_evaluated = subset.count()
    # Allow 1-minute buffer for timestamp precision nuances
    inversions = subset.filter(F.col(earlier_col) > F.col(later_col)).count()
    passed = (inversions == 0)
    return {
        "check_type": "TIMESTAMP_ORDER",
        "dataset": dataset_name,
        "comparison": f"{earlier_col} <= {later_col}",
        "total_evaluated": total_evaluated,
        "inversions_count": inversions,
        "status": "PASS" if passed else "WARN",
        "details": f"Found {inversions} timestamp order inversions between {earlier_col} and {later_col}"
    }


def check_referential_integrity(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_name: str,
    parent_name: str,
    foreign_key: str,
    primary_key: str
) -> Dict[str, Any]:
    """
    Performs referential integrity check between child and parent tables.
    Identifies orphan records (child rows whose foreign key does not exist in parent).
    IMPORTANT: Does NOT delete orphan records. Reports them for transparency.
    """
    total_child_rows = child_df.count()

    # Left anti join to find child rows not present in parent
    orphans_df = child_df.join(
        parent_df,
        child_df[foreign_key] == parent_df[primary_key],
        how="left_anti"
    )

    orphan_count = orphans_df.count()
    orphan_pct = round((orphan_count / total_child_rows * 100) if total_child_rows > 0 else 0.0, 4)

    # Collect up to 5 sample orphan keys for debugging
    sample_orphans = []
    if orphan_count > 0:
        sample_orphans = [
            row[foreign_key] for row in orphans_df.select(foreign_key).distinct().limit(5).collect()
        ]

    passed = (orphan_count == 0)
    result = {
        "check_type": "REFERENTIAL_INTEGRITY",
        "relationship": f"{child_name}.{foreign_key} -> {parent_name}.{primary_key}",
        "child_table": child_name,
        "parent_table": parent_name,
        "foreign_key": foreign_key,
        "primary_key": primary_key,
        "total_child_rows": total_child_rows,
        "orphan_count": orphan_count,
        "orphan_pct": orphan_pct,
        "sample_orphan_keys": sample_orphans,
        "status": "PASS" if passed else "FAIL",
        "details": (
            f"Referential integrity verified: 0 orphans"
            if passed else
            f"Found {orphan_count} orphan records ({orphan_pct}%) in {child_name} missing from {parent_name}."
        )
    }

    if not passed:
        logger.warning(
            f"[DQ ORPHANS] {child_name}.{foreign_key} -> {parent_name}.{primary_key}: "
            f"{orphan_count} orphans detected! Sample: {sample_orphans}"
        )
    else:
        logger.info(f"[DQ PASS] Referential integrity holds: {child_name}.{foreign_key} -> {parent_name}.{primary_key}")

    return result


def run_all_quality_checks(cleaned_datasets: Dict[str, DataFrame]) -> Dict[str, Any]:
    """
    Executes the comprehensive suite of data quality checks across all cleaned datasets.
    """
    logger.info("Executing Phase 3 Data Quality Check Suite...")
    dq_results = {
        "non_null_checks": [],
        "uniqueness_checks": [],
        "no_duplicate_checks": [],
        "non_negative_checks": [],
        "value_range_checks": [],
        "timestamp_checks": [],
        "referential_checks": [],
        "overall_status": "PASS"
    }

    # 1. Non-null primary/business keys
    key_definitions = {
        "customers": ["customer_id", "customer_unique_id"],
        "sellers": ["seller_id"],
        "orders": ["order_id", "customer_id"],
        "order_items": ["order_id", "order_item_id", "product_id", "seller_id"],
        "order_payments": ["order_id", "payment_sequential"],
        "order_reviews": ["review_id", "order_id"],
        "products": ["product_id"],
        "geolocation": ["geolocation_zip_code_prefix"],
    }
    for name, keys in key_definitions.items():
        if name in cleaned_datasets:
            res = check_non_null_keys(cleaned_datasets[name], name, keys)
            dq_results["non_null_checks"].extend(res)

    # 2. Uniqueness checks on primary entities
    unique_key_definitions = {
        "customers": ["customer_id"],
        "sellers": ["seller_id"],
        "orders": ["order_id"],
        "order_items": ["order_id", "order_item_id"],
        "products": ["product_id"],
        "geolocation": ["geolocation_zip_code_prefix"],
    }
    for name, keys in unique_key_definitions.items():
        if name in cleaned_datasets:
            res = check_uniqueness(cleaned_datasets[name], name, keys)
            dq_results["uniqueness_checks"].append(res)

    # 3. No exact duplicates across cleaned datasets
    for name, df in cleaned_datasets.items():
        res = check_no_exact_duplicates(df, name)
        dq_results["no_duplicate_checks"].append(res)

    # 4. Non-negative numeric values
    non_neg_definitions = {
        "order_items": ["price", "freight_value", "order_item_total"],
        "order_payments": ["payment_value", "payment_installments"],
        "products": ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    }
    for name, cols in non_neg_definitions.items():
        if name in cleaned_datasets:
            res = check_non_negative_values(cleaned_datasets[name], name, cols)
            dq_results["non_negative_checks"].extend(res)

    # 5. Value range checks (Review Score 1 to 5)
    if "order_reviews" in cleaned_datasets:
        res = check_value_range(cleaned_datasets["order_reviews"], "order_reviews", "review_score", 1, 5)
        dq_results["value_range_checks"].append(res)

    # 6. Timestamp order checks in orders
    if "orders" in cleaned_datasets:
        res1 = check_timestamp_order(cleaned_datasets["orders"], "orders", "order_purchase_timestamp", "order_delivered_customer_date")
        dq_results["timestamp_checks"].append(res1)

    # 7. Referential Integrity Relationships
    # orders -> customers
    if "orders" in cleaned_datasets and "customers" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["orders"], cleaned_datasets["customers"], "orders", "customers", "customer_id", "customer_id")
        )
    # order_items -> orders
    if "order_items" in cleaned_datasets and "orders" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["order_items"], cleaned_datasets["orders"], "order_items", "orders", "order_id", "order_id")
        )
    # order_items -> products
    if "order_items" in cleaned_datasets and "products" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["order_items"], cleaned_datasets["products"], "order_items", "products", "product_id", "product_id")
        )
    # order_items -> sellers
    if "order_items" in cleaned_datasets and "sellers" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["order_items"], cleaned_datasets["sellers"], "order_items", "sellers", "seller_id", "seller_id")
        )
    # payments -> orders
    if "order_payments" in cleaned_datasets and "orders" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["order_payments"], cleaned_datasets["orders"], "order_payments", "orders", "order_id", "order_id")
        )
    # reviews -> orders
    if "order_reviews" in cleaned_datasets and "orders" in cleaned_datasets:
        dq_results["referential_checks"].append(
            check_referential_integrity(cleaned_datasets["order_reviews"], cleaned_datasets["orders"], "order_reviews", "orders", "order_id", "order_id")
        )

    # Determine overall status: Any FAIL in non_null, uniqueness, non_negative, or range marks overall status
    critical_checks = (
        dq_results["non_null_checks"]
        + dq_results["uniqueness_checks"]
        + dq_results["no_duplicate_checks"]
        + dq_results["non_negative_checks"]
        + dq_results["value_range_checks"]
    )
    if any(c.get("status") == "FAIL" for c in critical_checks):
        dq_results["overall_status"] = "FAIL"
    else:
        dq_results["overall_status"] = "PASS"

    logger.info(f"Data quality checks completed. Overall status: {dq_results['overall_status']}")
    return dq_results
