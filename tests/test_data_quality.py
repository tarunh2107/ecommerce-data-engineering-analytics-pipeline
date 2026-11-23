"""
Unit tests for data quality and pipeline transformations.
Verifies that all processed Parquet datasets exist, have non-empty row counts,
contain derived business columns, and pass data quality assertions.
"""

import os
import pytest
import pyarrow.parquet as pq

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
ANALYTICS_DIR = os.path.join(PROJECT_ROOT, "data", "analytics")

EXPECTED_DATASETS = {
    "customers.parquet": 99441,
    "geolocation.parquet": 19015,
    "order_items.parquet": 112650,
    "order_payments.parquet": 103886,
    "order_reviews.parquet": 99224,
    "orders.parquet": 99441,
    "products.parquet": 32951,
    "sellers.parquet": 3095,
}


@pytest.mark.parametrize("dataset_name,expected_count", EXPECTED_DATASETS.items())
def test_processed_dataset_exists_and_count_matches(dataset_name, expected_count):
    """Verifies each processed Parquet dataset exists and matches row count expectations."""
    path = os.path.join(PROCESSED_DIR, dataset_name)
    assert os.path.exists(path), f"Processed Parquet dataset missing: {path}"

    table = pq.read_table(path)
    assert table.num_rows == expected_count, (
        f"Row count mismatch in {dataset_name}: expected {expected_count}, got {table.num_rows}"
    )


def test_orders_derived_columns():
    """Verifies that derived SLA and delivery metrics exist in orders.parquet."""
    path = os.path.join(PROCESSED_DIR, "orders.parquet")
    table = pq.read_table(path)
    cols = table.column_names

    assert "delivery_time_days" in cols
    assert "delivery_delay_days" in cols
    assert "is_late_delivery" in cols


def test_order_items_derived_column():
    """Verifies that gross item total exists in order_items.parquet."""
    path = os.path.join(PROCESSED_DIR, "order_items.parquet")
    table = pq.read_table(path)
    cols = table.column_names

    assert "order_item_total" in cols


def test_products_english_translation():
    """Verifies that English category names and corrected spelling exist in products.parquet."""
    path = os.path.join(PROCESSED_DIR, "products.parquet")
    table = pq.read_table(path)
    cols = table.column_names

    assert "product_category_name_english" in cols
    assert "product_name_length" in cols
    assert "product_description_length" in cols
    assert "product_name_lenght" not in cols


def test_transformation_report_generated():
    """Verifies that transformation report exists and marked PASS."""
    report_path = os.path.join(ANALYTICS_DIR, "transformation_report.txt")
    assert os.path.exists(report_path), f"Transformation report missing: {report_path}"

    with open(report_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "Overall Phase 3 DQ Status: PASS" in content
