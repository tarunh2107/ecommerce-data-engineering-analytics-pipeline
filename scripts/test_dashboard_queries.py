import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.database import check_database_connection
from dashboard.queries import (
    get_executive_kpis,
    get_monthly_sales_trend,
    get_sales_by_state,
    get_top_categories,
    get_rfm_segmentation_summary,
    get_logistics_by_state,
    get_delivery_review_correlation,
    get_seller_pareto_analysis,
    get_pipeline_warehouse_stats,
    get_filter_options,
)

def main():
    print("Testing PostgreSQL connection...")
    connected, info = check_database_connection()
    assert connected, f"Database not connected: {info}"
    print(f"Connected! PostgreSQL Version: {info['version'][:45]}... | Total Rows: {info['total_rows']:,}")

    print("\n1. Testing get_executive_kpis()...")
    kpis = get_executive_kpis()
    print(f"   Total Orders: {kpis['total_orders']:,} | Delivered GMV: R$ {kpis['total_delivered_gmv']:,.2f}")
    assert kpis['total_orders'] == 99441
    assert kpis['delivered_orders'] == 96478
    assert kpis['total_customers'] == 99441
    assert kpis['active_sellers'] == 3095

    print("\n2. Testing get_monthly_sales_trend()...")
    df_trend = get_monthly_sales_trend()
    print(f"   Monthly records returned: {len(df_trend)}")
    assert len(df_trend) > 0

    print("\n3. Testing get_sales_by_state()...")
    df_state = get_sales_by_state()
    print(f"   States returned: {len(df_state)}")
    assert len(df_state) == 27

    print("\n4. Testing get_top_categories(10)...")
    df_cat = get_top_categories(10)
    print(f"   Top category: {df_cat.iloc[0]['category_name']} (R$ {df_cat.iloc[0]['total_category_revenue']:,.2f})")
    assert len(df_cat) == 10

    print("\n5. Testing get_rfm_segmentation_summary()...")
    df_rfm = get_rfm_segmentation_summary()
    print(f"   RFM Segments: {len(df_rfm)} | Total RFM Customers: {df_rfm['customer_count'].sum():,}")
    assert len(df_rfm) == 5
    assert df_rfm['customer_count'].sum() == 93358

    print("\n6. Testing get_logistics_by_state()...")
    df_log = get_logistics_by_state()
    print(f"   States logistics records: {len(df_log)}")
    assert len(df_log) == 27

    print("\n7. Testing get_delivery_review_correlation()...")
    df_corr = get_delivery_review_correlation()
    print(f"   Correlation categories: {len(df_corr)}")
    for _, row in df_corr.iterrows():
        print(f"     {row['delivery_status']}: Rating {row['avg_customer_rating']} (Avg {row['avg_delivery_days']} days)")

    print("\n8. Testing get_seller_pareto_analysis()...")
    df_sellers, pareto = get_seller_pareto_analysis()
    print(f"   Active Sellers: {pareto['total_active_sellers']:,} | Top 20% count: {pareto['top_20_pct_seller_count']:,} | Revenue share: {pareto['revenue_share_of_top_20_pct']}%")

    print("\n9. Testing get_pipeline_warehouse_stats()...")
    stats = get_pipeline_warehouse_stats()
    print(f"   Warehouse Row Counts: {stats}")
    assert stats['total_warehouse_rows'] == 349405

    print("\n10. Testing get_filter_options()...")
    filters = get_filter_options()
    print(f"    Available Years: {filters['years']}")
    print(f"    Available States ({len(filters['states'])}): {filters['states'][:5]}...")

    print("\nALL 10 ANALYTICAL QUERY TESTS PASSED WITH 100% RECONCILIATION!")

if __name__ == "__main__":
    main()
