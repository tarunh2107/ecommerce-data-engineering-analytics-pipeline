"""
Module: ingest_raw
Description: Ingestion, schema validation, and profiling for raw Olist CSV datasets.
             Discovers raw files, verifies existence, calculates dimensions, data types,
             duplicate rows, and null value distributions, and generates a profiling report.
             NOTE: No cleaning or transformation is performed in this phase.
"""

import os
import sys
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

# Ensure project root is in sys.path when running script directly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("raw_ingestion_validator")

# Expected schemas for raw Olist dataset files
EXPECTED_SCHEMAS: Dict[str, List[str]] = {
    "olist_customers_dataset.csv": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "olist_geolocation_dataset.csv": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    "olist_order_items_dataset.csv": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ],
    "olist_order_payments_dataset.csv": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "olist_order_reviews_dataset.csv": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "olist_orders_dataset.csv": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_products_dataset.csv": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
    "olist_sellers_dataset.csv": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "product_category_name_translation.csv": [
        "product_category_name",
        "product_category_name_english",
    ],
}


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Loads configuration YAML file."""
    if config_path is None:
        config_path = os.path.join(PROJECT_ROOT, "config", "config.yaml")

    if not os.path.exists(config_path):
        logger.warning(f"Config file not found at {config_path}. Using default paths.")
        return {
            "storage": {"raw_dir": "data/raw", "analytics_dir": "data/analytics"},
            "raw_files": {k.split("_")[1]: k for k in EXPECTED_SCHEMAS.keys()},
        }

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def profile_single_dataset(file_name: str, file_path: str) -> Dict[str, Any]:
    """
    Profiles a single CSV dataset:
    - Verifies file presence and size
    - Computes shape (rows, columns)
    - Detects column names and missing expected columns
    - Calculates duplicate rows
    - Calculates null counts and null percentages
    - Inspects column data types
    """
    expected_cols = EXPECTED_SCHEMAS.get(file_name, [])
    result: Dict[str, Any] = {
        "file_name": file_name,
        "file_path": file_path,
        "exists": False,
        "file_size_bytes": 0,
        "rows": 0,
        "cols": 0,
        "columns": [],
        "missing_columns": [],
        "duplicate_rows": 0,
        "null_details": {},
        "dtypes": {},
        "status": "FAIL",
        "failure_reasons": [],
    }

    if not os.path.exists(file_path):
        result["failure_reasons"].append(f"File does not exist: {file_path}")
        return result

    result["exists"] = True
    result["file_size_bytes"] = os.path.getsize(file_path)

    if result["file_size_bytes"] == 0:
        result["failure_reasons"].append("File is empty (0 bytes)")
        return result

    try:
        # Load CSV for profiling (read all as string initially or infer types safely)
        df = pd.read_csv(file_path, low_memory=False)
        result["rows"], result["cols"] = df.shape
        actual_cols = list(df.columns)
        result["columns"] = actual_cols

        # Check for missing expected columns
        missing_cols = [c for c in expected_cols if c not in actual_cols]
        result["missing_columns"] = missing_cols
        if missing_cols:
            result["failure_reasons"].append(f"Missing expected columns: {missing_cols}")

        # Check for duplicate rows
        result["duplicate_rows"] = int(df.duplicated().sum())

        # Check null counts and null percentages per column
        null_counts = df.isnull().sum()
        null_pcts = (null_counts / len(df)) * 100 if len(df) > 0 else null_counts

        null_details = {}
        for col in actual_cols:
            null_details[col] = {
                "null_count": int(null_counts[col]),
                "null_pct": round(float(null_pcts[col]), 2),
                "dtype": str(df[col].dtype),
            }
        result["null_details"] = null_details
        result["dtypes"] = {col: str(df[col].dtype) for col in actual_cols}

        # Status evaluation
        if result["rows"] > 0 and len(missing_cols) == 0:
            result["status"] = "PASS"
        else:
            if result["rows"] == 0:
                result["failure_reasons"].append("Dataset contains 0 rows")

    except Exception as e:
        result["failure_reasons"].append(f"Error reading dataset: {str(e)}")

    return result


def generate_profile_report(profiles: List[Dict[str, Any]], output_path: str) -> None:
    """Writes a formatted profiling and validation report to a text file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_datasets = len(profiles)
    passed_datasets = sum(1 for p in profiles if p["status"] == "PASS")
    overall_status = "PASS" if passed_datasets == total_datasets else "FAIL"

    lines = []
    lines.append("=" * 80)
    lines.append("OLIST E-COMMERCE DATASET - RAW DATA PROFILING & VALIDATION REPORT")
    lines.append(f"Generated at: {timestamp}")
    lines.append(f"Python Version: {sys.version.split()[0]}")
    lines.append(f"Overall Phase 2 Status: {overall_status} ({passed_datasets}/{total_datasets} Datasets Passed)")
    lines.append("=" * 80)
    lines.append("")

    # Summary Table
    lines.append("EXECUTIVE SUMMARY TABLE:")
    lines.append("-" * 80)
    header = f"{'Dataset Name':<38} | {'Rows':>8} | {'Cols':>4} | {'Duplicates':>10} | {'Status':<6}"
    lines.append(header)
    lines.append("-" * 80)

    for p in profiles:
        file_name = p["file_name"]
        rows = f"{p['rows']:,}" if p["exists"] else "N/A"
        cols = str(p["cols"]) if p["exists"] else "N/A"
        dups = f"{p['duplicate_rows']:,}" if p["exists"] else "N/A"
        status = p["status"]
        lines.append(f"{file_name:<38} | {rows:>8} | {cols:>4} | {dups:>10} | {status:<6}")

    lines.append("-" * 80)
    lines.append("")

    # Detailed Profiling Section
    lines.append("DETAILED DATASET PROFILES & NULL VALUE DISTRIBUTIONS:")
    lines.append("=" * 80)

    for p in profiles:
        lines.append("")
        lines.append(f"Dataset: {p['file_name']}")
        lines.append(f"  Status          : {p['status']}")
        lines.append(f"  File Size       : {p['file_size_bytes']:,} bytes ({p['file_size_bytes'] / (1024*1024):.2f} MB)")
        lines.append(f"  Total Rows      : {p['rows']:,}")
        lines.append(f"  Total Columns   : {p['cols']}")
        lines.append(f"  Duplicate Rows  : {p['duplicate_rows']:,} ({ (p['duplicate_rows']/p['rows']*100 if p['rows'] > 0 else 0):.2f}%)")

        if p["failure_reasons"]:
            lines.append(f"  Failure Reasons : {'; '.join(p['failure_reasons'])}")

        lines.append("  Columns Breakdown:")
        lines.append(f"    {'Column Name':<34} | {'Dtype':<10} | {'Null Count':>10} | {'Null %':>8}")
        lines.append(f"    {'-'*34}-|-{'-'*10}-|-{'-'*10}-|-{'-'*8}")

        # Highlight major null columns
        major_nulls = []
        for col, meta in p["null_details"].items():
            col_name = col[:34]
            dtype = meta["dtype"]
            null_cnt = f"{meta['null_count']:,}"
            null_pct = f"{meta['null_pct']:.2f}%"
            lines.append(f"    {col_name:<34} | {dtype:<10} | {null_cnt:>10} | {null_pct:>8}")
            if meta["null_pct"] > 0:
                major_nulls.append(f"{col} ({meta['null_pct']:.2f}%)")

        if major_nulls:
            lines.append(f"  * Columns with missing values: {', '.join(major_nulls)}")
        else:
            lines.append("  * Columns with missing values: None (Complete dataset)")

        lines.append("-" * 80)

    lines.append("")
    lines.append("END OF PROFILING REPORT")
    lines.append("=" * 80)

    report_text = "\n".join(lines)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    logger.info(f"Profiling report written to: {output_path}")


def run_raw_ingestion_validation() -> bool:
    """
    Main orchestration function for Phase 2:
    - Reads config
    - Discovers all 9 datasets
    - Profiles each dataset
    - Displays validation output
    - Generates profiling report
    """
    logger.info("Starting Phase 2: Raw Data Ingestion Validation & Profiling")
    config = load_config()

    raw_dir = config.get("storage", {}).get("raw_dir", "data/raw")
    analytics_dir = config.get("storage", {}).get("analytics_dir", "data/analytics")

    raw_dir_abs = os.path.join(PROJECT_ROOT, raw_dir)
    analytics_dir_abs = os.path.join(PROJECT_ROOT, analytics_dir)
    report_file_abs = os.path.join(analytics_dir_abs, "raw_data_profile.txt")

    raw_files_dict = config.get("raw_files", {})
    file_list = list(raw_files_dict.values()) if raw_files_dict else list(EXPECTED_SCHEMAS.keys())

    profiles = []
    all_passed = True

    logger.info(f"Discovered {len(file_list)} expected datasets to validate in: {raw_dir_abs}")

    for file_name in file_list:
        file_path = os.path.join(raw_dir_abs, file_name)
        profile = profile_single_dataset(file_name, file_path)
        profiles.append(profile)

        status = profile["status"]
        if status == "PASS":
            logger.info(
                f"[PASS] {file_name:<38} | Shape: ({profile['rows']:,}, {profile['cols']}) | "
                f"Duplicates: {profile['duplicate_rows']:,}"
            )
        else:
            all_passed = False
            logger.error(
                f"[FAIL] {file_name:<38} | Reasons: {'; '.join(profile['failure_reasons'])}"
            )

    # Generate profiling report
    generate_profile_report(profiles, report_file_abs)

    if all_passed:
        logger.info(">>> ALL 9 DATASETS PASSED VALIDATION! Phase 2 Ingestion Complete. <<<")
    else:
        logger.error(">>> ONE OR MORE DATASETS FAILED VALIDATION. Check report details. <<<")

    return all_passed


if __name__ == "__main__":
    success = run_raw_ingestion_validation()
    sys.exit(0 if success else 1)
