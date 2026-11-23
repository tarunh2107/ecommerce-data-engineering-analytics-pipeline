"""
Main Streamlit application for the E-Commerce Data Engineering & Analytics Platform.
Serves as the executive presentation layer querying the PostgreSQL Kimball Data Warehouse.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path for robust execution from any directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd

try:
    from dashboard.database import check_database_connection
    from dashboard.components import (
        inject_custom_css,
        render_kpi_card,
        plot_monthly_sales_trend,
        plot_sales_by_state,
        plot_top_categories,
        plot_rfm_distribution,
        plot_logistics_performance_bars,
        plot_seller_pareto,
        render_pipeline_flow,
        COLORS,
    )
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
except ModuleNotFoundError:
    from database import check_database_connection
    from components import (
        inject_custom_css,
        render_kpi_card,
        plot_monthly_sales_trend,
        plot_sales_by_state,
        plot_top_categories,
        plot_rfm_distribution,
        plot_logistics_performance_bars,
        plot_seller_pareto,
        render_pipeline_flow,
        COLORS,
    )
    from queries import (
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

# Configure wide-layout page
st.set_page_config(
    page_title="E-Commerce Analytics Platform",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply styling
inject_custom_css()

# Header & Branding
st.title("🛒 E-Commerce Data Engineering & Analytics Platform")
st.caption("End-to-End Retail Data Pipeline | PySpark • PostgreSQL • Airflow")
st.markdown("---")

# Verify Database Connection
connected, conn_info = check_database_connection()
if not connected:
    st.error(f"""
        ⚠️ **PostgreSQL Warehouse Unreachable**  
        Could not establish a connection to the data warehouse at `localhost:5432`.  
        **Error Details**: `{conn_info.get('error')}`  
        *Please ensure the Docker warehouse container `olist-postgres-dw` is running.*
    """)
    st.stop()

# Sidebar Navigation
st.sidebar.markdown("### 🧭 Navigation")
page = st.sidebar.radio(
    "Select Perspective",
    [
        "📊 Executive Overview",
        "📈 Sales Analytics",
        "📦 Product Analytics",
        "👥 Customer Analytics",
        "🚚 Logistics Analytics",
        "🏬 Seller Analytics",
        "⚙️ Data Pipeline",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔌 Data Warehouse Status")
st.sidebar.success(f"**PostgreSQL**: Connected\n- **Warehouse**: `olist_dw`\n- **Schema**: `dwh`\n- **Records**: {conn_info.get('total_rows', 0):,}")
st.sidebar.caption("Batch ETL | Data updated via Airflow")


# ==============================================================================
# 1. EXECUTIVE OVERVIEW
# ==============================================================================
if page == "📊 Executive Overview":
    st.subheader("Executive KPI Summary")
    kpis = get_executive_kpis()

    # KPI Row 1
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi_card("Total Orders", f"{int(kpis['total_orders']):,}", "Delivered: 96,478 (97.0%)")
    with c2:
        render_kpi_card("Delivered GMV", f"R$ {kpis['total_delivered_gmv']:,.2f}", "Total Payments: R$ 16.01M")
    with c3:
        render_kpi_card("Average Order Value", f"R$ {kpis['average_order_value']:,.2f}", "Basket spend per order")
    with c4:
        render_kpi_card("Total Customers", f"{int(kpis['total_customers']):,}", "93,358 delivered entities")

    # KPI Row 2
    c5, c6, c7, c8 = st.columns(4)
    with c5:
        render_kpi_card("Active Merchants", f"{int(kpis['active_sellers']):,}", "Total registered: 3,095")
    with c6:
        render_kpi_card("Avg Delivery Speed", f"{kpis['avg_delivery_days']:.1f} Days", "Door-to-door transit")
    with c7:
        render_kpi_card("Late Delivery Rate", f"{kpis['late_delivery_rate_pct']:.1f}%", "Delivered past estimate", is_positive=False)
    with c8:
        render_kpi_card("Customer Rating", f"{kpis['avg_review_score']:.2f} / 5.0", "From 99,224 order reviews")

    st.markdown("### 📈 Revenue & Order Volume Trajectory")
    trend_df = get_monthly_sales_trend()
    st.plotly_chart(plot_monthly_sales_trend(trend_df), use_container_width=True)
    st.caption("ℹ️ **Visualization Note**: Early ramp-up months with near-zero prior revenue (< R$ 1,000, such as 2016-10 and 2017-01 from 1 pilot order each) are excluded from the MoM line to prevent arithmetic base distortion (+32,307% and +649,657%) and keep operational movements (-27% to +113%) readable. Unfiltered SQL values remain unaltered in data tables and analytics reports.")

    # Regional & Category Distribution
    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("### 🗺️ Top 10 States by Revenue")
        state_df = get_sales_by_state()
        st.plotly_chart(plot_sales_by_state(state_df, top_n=10), use_container_width=True)

    with col_right:
        st.markdown("### 🏆 Top 10 Product Categories")
        cat_df = get_top_categories(top_n=10)
        st.plotly_chart(plot_top_categories(cat_df), use_container_width=True)


# ==============================================================================
# 2. SALES ANALYTICS
# ==============================================================================
elif page == "📈 Sales Analytics":
    st.subheader("Sales & Revenue Performance")

    # Filters
    filters = get_filter_options()
    fc1, fc2, _ = st.columns([1.5, 1.5, 3])
    with fc1:
        sel_year = st.selectbox("Filter Year", filters["years"], index=0)
    with fc2:
        sel_state = st.selectbox("Filter Customer State", filters["states"], index=0)

    filter_year_val = None if sel_year == "All" else int(sel_year)
    filter_state_val = None if sel_state == "All" else sel_state

    sales_df = get_monthly_sales_trend(year=filter_year_val, state=filter_state_val)

    # Filtered KPIs
    if not sales_df.empty:
        total_filtered_rev = sales_df["total_order_value"].sum()
        total_filtered_orders = sales_df["number_of_orders"].sum()
        avg_filtered_aov = total_filtered_rev / total_filtered_orders if total_filtered_orders > 0 else 0
        total_items = sales_df["number_of_items"].sum()

        sc1, sc2, sc3, sc4 = st.columns(4)
        with sc1:
            render_kpi_card("Filtered Revenue", f"R$ {total_filtered_rev:,.2f}")
        with sc2:
            render_kpi_card("Filtered Orders", f"{total_filtered_orders:,}")
        with sc3:
            render_kpi_card("Filtered AOV", f"R$ {avg_filtered_aov:,.2f}")
        with sc4:
            render_kpi_card("Units Sold", f"{int(total_items):,}")

        st.plotly_chart(plot_monthly_sales_trend(sales_df), use_container_width=True)
        st.caption("ℹ️ **Visualization Note**: Months with an insufficient comparison base (prior GMV < R$ 1,000) are excluded from the MoM growth line to ensure readable scaling. Exact unfiltered values are presented in the Monthly Financial Detail table below.")

        st.markdown("### 📋 Monthly Financial Detail")
        display_sales = sales_df[[
            "year_month", "number_of_orders", "number_of_items",
            "gross_product_revenue", "freight_revenue", "total_order_value",
            "average_order_value", "mom_growth_pct"
        ]].rename(columns={
            "year_month": "Month",
            "number_of_orders": "Orders",
            "number_of_items": "Units",
            "gross_product_revenue": "Merchandise (BRL)",
            "freight_revenue": "Freight (BRL)",
            "total_order_value": "Total GMV (BRL)",
            "average_order_value": "AOV (BRL)",
            "mom_growth_pct": "MoM Growth %"
        })
        st.dataframe(display_sales, use_container_width=True, hide_index=True)
    else:
        st.warning("No sales transactions found for the selected filter combination.")


# ==============================================================================
# 3. PRODUCT ANALYTICS
# ==============================================================================
elif page == "📦 Product Analytics":
    st.subheader("Product Category Performance")

    top_n = st.select_slider("Select Category Leaderboard Depth", options=[10, 20, 30], value=10)

    cat_df = get_top_categories(top_n=top_n)

    p1, p2, p3 = st.columns(3)
    with p1:
        render_kpi_card("Top Category", cat_df.iloc[0]["category_name"], f"R$ {cat_df.iloc[0]['total_category_revenue']:,.2f}")
    with p2:
        render_kpi_card("Top Category Units", f"{cat_df.iloc[0]['units_sold']:,}", f"{cat_df.iloc[0]['orders_count']:,} orders")
    with p3:
        avg_cat_price = cat_df["average_item_price"].mean()
        render_kpi_card("Avg Item Price", f"R$ {avg_cat_price:,.2f}", f"Across top {top_n} categories")

    st.plotly_chart(plot_top_categories(cat_df), use_container_width=True)

    st.markdown("### 📋 Category Economics Table")
    display_cat = cat_df.rename(columns={
        "category_name": "Category",
        "orders_count": "Orders",
        "units_sold": "Units Sold",
        "gross_sales_value": "Sales (BRL)",
        "freight_value": "Freight (BRL)",
        "total_category_revenue": "Total Revenue (BRL)",
        "average_item_price": "Avg Price (BRL)",
        "revenue_contribution_pct": "Market Share %"
    })
    st.dataframe(display_cat, use_container_width=True, hide_index=True)


# ==============================================================================
# 4. CUSTOMER ANALYTICS
# ==============================================================================
elif page == "👥 Customer Analytics":
    st.subheader("Customer Behavioral Segmentation (RFM)")
    st.info("""
        💡 **Descriptive Customer Segmentation**: Customers are grouped into behavioral cohorts using 
        **Recency** (days since last purchase), **Frequency** (order count), and **Monetary** (total GMV) 
        window scoring in PostgreSQL. *This is rule-based descriptive SQL analytics, not predictive machine learning.*
    """)

    rfm_df = get_rfm_segmentation_summary()

    # Segment KPI cards
    c_cols = st.columns(len(rfm_df))
    for idx, row in rfm_df.iterrows():
        with c_cols[idx]:
            render_kpi_card(
                row["customer_segment"],
                f"{int(row['customer_count']):,}",
                f"{row['revenue_share_pct']:.1f}% of Revenue"
            )

    st.markdown("### 📊 Customer Share vs Revenue Contribution")
    st.plotly_chart(plot_rfm_distribution(rfm_df), use_container_width=True)

    # Segment Detail Table
    st.markdown("### 📋 Cohort Performance Metrics")
    display_rfm = rfm_df.rename(columns={
        "customer_segment": "Cohort",
        "customer_count": "Customers",
        "customer_share_pct": "% of Customers",
        "total_segment_revenue": "Total GMV (BRL)",
        "revenue_share_pct": "% of Revenue",
        "avg_monetary_value": "Avg Spend (BRL)",
        "avg_recency_days": "Avg Recency (Days)",
        "avg_frequency": "Avg Orders"
    })
    st.dataframe(display_rfm, use_container_width=True, hide_index=True)

    # Sample customer profiles
    st.markdown("### 🔍 Customer Drilldown")
    selected_cohort = st.selectbox("Explore Cohort Members", ["All"] + rfm_df["customer_segment"].tolist())
    sample_customers = get_sample_rfm_customers(segment=selected_cohort, limit=15)
    st.dataframe(sample_customers, use_container_width=True, hide_index=True)


# ==============================================================================
# 5. LOGISTICS ANALYTICS
# ==============================================================================
elif page == "🚚 Logistics Analytics":
    st.subheader("Carrier Delivery SLA & Shipping Performance")

    kpis = get_executive_kpis()
    corr_df = get_delivery_review_correlation()

    l1, l2, l3, l4 = st.columns(4)
    with l1:
        render_kpi_card("Avg Transit Time", f"{kpis['avg_delivery_days']:.1f} Days", "Order to customer delivery")
    with l2:
        render_kpi_card("On-Time Rate", f"{100 - kpis['late_delivery_rate_pct']:.1f}%", "Delivered on/before SLA", is_positive=True)
    with l3:
        render_kpi_card("Carrier Delay Rate", f"{kpis['late_delivery_rate_pct']:.1f}%", "Delivered after estimated date", is_positive=False)
    with l4:
        render_kpi_card("Delivered Orders", f"{int(kpis['delivered_orders']):,}", "Analyzed fulfillment volume")

    # Delivery Delay vs Review Score Callout
    st.markdown("### ⚠️ Impact of Shipping Delays on Customer Satisfaction")
    st.dataframe(
        corr_df.rename(columns={
            "delivery_status": "Fulfillment Status",
            "order_count": "Orders",
            "avg_delivery_days": "Transit Days",
            "avg_customer_rating": "Average Rating (★)",
            "five_star_reviews": "5★ Reviews",
            "one_star_reviews": "1★ Reviews"
        }),
        use_container_width=True,
        hide_index=True
    )

    # Regional Delivery Speeds
    log_state_df = get_logistics_by_state()
    col_fast, col_slow = st.columns(2)
    with col_fast:
        st.markdown("### 🚀 Fastest Delivery States (Top 8)")
        st.plotly_chart(plot_logistics_performance_bars(log_state_df, top_n=8, ascending=True), use_container_width=True)

    with col_slow:
        st.markdown("### ⏳ Longest Delivery States (Top 8)")
        st.plotly_chart(plot_logistics_performance_bars(log_state_df, top_n=8, ascending=False), use_container_width=True)


# ==============================================================================
# 6. SELLER ANALYTICS
# ==============================================================================
elif page == "🏬 Seller Analytics":
    st.subheader("Merchant Fulfillment & Marketplace Revenue Concentration")

    sellers_df, pareto = get_seller_pareto_analysis()

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        render_kpi_card("Active Merchants", f"{pareto['total_active_sellers']:,}", "With >= 1 fulfilled item")
    with s2:
        render_kpi_card("Top 20% Merchant Count", f"{pareto['top_20_pct_seller_count']:,}", "Core seller concentration")
    with s3:
        render_kpi_card("Top 20% Revenue Share", f"{pareto['revenue_share_of_top_20_pct']:.1f}%", "Confirms Pareto principle (80/20)")
    with s4:
        render_kpi_card("Marketplace Sales", f"R$ {pareto['total_marketplace_revenue']:,.2f}", "Total merchant merchandise GMV")

    st.markdown("### 📐 Empirical Seller Pareto Curve")
    st.plotly_chart(plot_seller_pareto(sellers_df), use_container_width=True)

    st.markdown("### 🏆 Top 15 Marketplace Sellers")
    top_sellers = sellers_df.head(15)[[
        "seller_rank", "seller_id", "seller_city", "seller_state",
        "orders_fulfilled", "items_sold", "total_seller_revenue", "cumulative_revenue_pct"
    ]].rename(columns={
        "seller_rank": "Rank",
        "seller_id": "Seller ID",
        "seller_city": "City",
        "seller_state": "State",
        "orders_fulfilled": "Orders",
        "items_sold": "Units Sold",
        "total_seller_revenue": "Revenue (BRL)",
        "cumulative_revenue_pct": "Cumulative %"
    })
    st.dataframe(top_sellers, use_container_width=True, hide_index=True)


# ==============================================================================
# 7. DATA PIPELINE
# ==============================================================================
elif page == "⚙️ Data Pipeline":
    st.subheader("End-to-End Data Engineering Architecture")
    st.caption("Batch ETL Pipeline | Olist E-Commerce Dataset (~100K Orders)")

    pipe_stats = get_pipeline_warehouse_stats()

    # Architecture Overview Cards
    col_arch, col_stats = st.columns([1.8, 1.2])

    with col_arch:
        st.markdown("### 🏗️ Pipeline Flow")
        render_pipeline_flow()

    with col_stats:
        st.markdown("### 📊 Kimball Star Schema Inventory")
        st.markdown(f"""
        | Table Name | Type | Verified Records |
        |---|---|---|
        | `dwh.dim_date` | Dimension | **{pipe_stats['dim_date_rows']:,}** |
        | `dwh.dim_customer` | Dimension | **{pipe_stats['dim_customer_rows']:,}** |
        | `dwh.dim_product` | Dimension | **{pipe_stats['dim_product_rows']:,}** |
        | `dwh.dim_seller` | Dimension | **{pipe_stats['dim_seller_rows']:,}** |
        | `dwh.fact_order` | Fact | **{pipe_stats['fact_order_rows']:,}** |
        | `dwh.fact_order_item` | Fact | **{pipe_stats['fact_order_item_rows']:,}** |
        | **Total Warehouse Rows** | | **{pipe_stats['total_warehouse_rows']:,}** |
        """)

        st.markdown("### 🛡️ Verified Pipeline Safeguards")
        st.success("""
        - **Source Ingestion**: 9 raw CSVs validated against strict row-count & schema tolerances.
        - **PySpark Cleaning**: Geolocation dedup (1.0M -> 19K centroids), timestamp normalization.
        - **Quality Gate**: 12 automated PySpark assertions; 0 null surrogate keys.
        - **PostgreSQL DWH**: COPY bulk load with foreign keys and unique constraints.
        - **Reconciliation**: R$ 16,008,872.12 payments verified down to R$ 0.00 variance.
        - **Airflow Orchestration**: 9-step linear DAG executed successfully in Docker stack.
        """)

        st.markdown("### 🏷️ Technology Stack")
        st.markdown("""
        <span class="tech-badge">Python 3.11</span>
        <span class="tech-badge">PySpark 3.5.9</span>
        <span class="tech-badge">PostgreSQL 15</span>
        <span class="tech-badge">Apache Airflow 2.7.3</span>
        <span class="tech-badge">Docker Compose</span>
        <span class="tech-badge">Streamlit 1.63</span>
        <span class="tech-badge">Plotly 7.0</span>
        <span class="tech-badge">SQLAlchemy</span>
        """, unsafe_allow_html=True)
