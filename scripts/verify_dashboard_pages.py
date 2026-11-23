"""
Automated verification script for all 7 dashboard page components and charts.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.database import check_database_connection
from dashboard.queries import (
    get_executive_kpis,
    get_monthly_sales_trend,
    get_sales_by_state,
    get_top_categories,
    get_rfm_segmentation_summary,
    get_sample_rfm_customers,
    get_logistics_by_state,
    get_delivery_review_correlation,
    get_seller_pareto_analysis,
    get_pipeline_warehouse_stats,
    get_filter_options,
)
from dashboard.components import (
    plot_monthly_sales_trend,
    plot_sales_by_state,
    plot_top_categories,
    plot_rfm_distribution,
    plot_logistics_performance_bars,
    plot_seller_pareto,
)

def test_all_pages():
    print("Testing connection...")
    connected, info = check_database_connection()
    assert connected, f"Connection failed: {info}"
    print("DB connection verified.")

    print("\n--- Testing Page 1: Executive Overview ---")
    kpis = get_executive_kpis()
    trend_df = get_monthly_sales_trend()
    fig1 = plot_monthly_sales_trend(trend_df)
    state_df = get_sales_by_state()
    fig2 = plot_sales_by_state(state_df, top_n=10)
    cat_df = get_top_categories(top_n=10)
    fig3 = plot_top_categories(cat_df)
    assert fig1 is not None and fig2 is not None and fig3 is not None
    print("Page 1 charts and metrics OK.")

    print("\n--- Testing Page 2: Sales Analytics ---")
    filters = get_filter_options()
    filtered_trend = get_monthly_sales_trend(year=2017, state="SP")
    fig_sales = plot_monthly_sales_trend(filtered_trend)
    assert fig_sales is not None
    print(f"Page 2 filtered sales (2017, SP): {len(filtered_trend)} months OK.")

    print("\n--- Testing Page 3: Product Analytics ---")
    for top_n in [10, 20, 30]:
        df_top = get_top_categories(top_n=top_n)
        fig_cat = plot_top_categories(df_top)
        assert len(df_top) == top_n and fig_cat is not None
    print("Page 3 top 10/20/30 category charts OK.")

    print("\n--- Testing Page 4: Customer Analytics (RFM) ---")
    rfm_df = get_rfm_segmentation_summary()
    fig_rfm = plot_rfm_distribution(rfm_df)
    sample_champions = get_sample_rfm_customers(segment="Champions", limit=10)
    assert fig_rfm is not None and len(sample_champions) == 10
    print("Page 4 RFM treemap and cohort profiles OK.")

    print("\n--- Testing Page 5: Logistics Analytics ---")
    corr_df = get_delivery_review_correlation()
    log_state_df = get_logistics_by_state()
    fig_fast = plot_logistics_performance_bars(log_state_df, top_n=8, ascending=True)
    fig_slow = plot_logistics_performance_bars(log_state_df, top_n=8, ascending=False)
    assert len(corr_df) == 2 and fig_fast is not None and fig_slow is not None
    print("Page 5 logistics speed rankings and delay impact OK.")

    print("\n--- Testing Page 6: Seller Analytics (Pareto) ---")
    sellers_df, pareto = get_seller_pareto_analysis()
    fig_pareto = plot_seller_pareto(sellers_df)
    assert fig_pareto is not None
    assert pareto["revenue_share_of_top_20_pct"] > 80.0
    print(f"Page 6 Pareto curve OK: Top 20% ({pareto['top_20_pct_seller_count']}) generate {pareto['revenue_share_of_top_20_pct']}% revenue.")

    print("\n--- Testing Page 7: Data Pipeline ---")
    pipe_stats = get_pipeline_warehouse_stats()
    assert pipe_stats["total_warehouse_rows"] == 349405
    print("Page 7 warehouse stats OK (349,405 rows).")

    print("\nALL 7 DASHBOARD PAGES AND PLOTLY CHARTS VERIFIED SUCCESSFULLY!")

if __name__ == "__main__":
    test_all_pages()
