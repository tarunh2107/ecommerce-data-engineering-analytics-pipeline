"""
Analytical SQL queries for the Streamlit analytics dashboard.
All heavy aggregations execute inside PostgreSQL and are cached via Streamlit.
"""

import sys
from pathlib import Path

# Add project root directory to sys.path for robust execution from any directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import streamlit as st

try:
    from dashboard.database import run_query
except ModuleNotFoundError:
    from database import run_query


@st.cache_data(ttl=3600)
def get_executive_kpis() -> Dict[str, Any]:
    """Retrieves top-level executive KPIs reconciled against the warehouse."""
    sql = """
    SELECT
        COUNT(*) AS total_orders,
        COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) AS delivered_orders,
        COUNT(CASE WHEN order_status = 'canceled' THEN 1 END) AS canceled_orders,
        (SELECT COUNT(*) FROM dwh.dim_customer) AS total_customers,
        (SELECT COUNT(*) FROM dwh.dim_seller) AS total_sellers,
        (SELECT COUNT(DISTINCT seller_key) FROM dwh.fact_order_item) AS active_sellers,
        (SELECT COUNT(*) FROM dwh.dim_product) AS total_products,
        ROUND(SUM(CASE WHEN order_status = 'delivered' THEN total_price ELSE 0 END), 2) AS gross_merchandise_value,
        ROUND(SUM(CASE WHEN order_status = 'delivered' THEN total_freight ELSE 0 END), 2) AS freight_revenue,
        ROUND(SUM(CASE WHEN order_status = 'delivered' THEN total_order_value ELSE 0 END), 2) AS total_delivered_gmv,
        ROUND(SUM(total_payment), 2) AS total_payments,
        ROUND(AVG(CASE WHEN order_status = 'delivered' THEN total_order_value END), 2) AS average_order_value,
        ROUND(AVG(CASE WHEN order_status = 'delivered' THEN delivery_time_days END), 1) AS avg_delivery_days,
        ROUND(
            COUNT(CASE WHEN order_status = 'delivered' AND delivery_delay_days > 0 THEN 1 END)::NUMERIC 
            / NULLIF(COUNT(CASE WHEN order_status = 'delivered' AND delivery_time_days IS NOT NULL THEN 1 END), 0) * 100, 
            2
        ) AS late_delivery_rate_pct,
        ROUND(AVG(review_score), 2) AS avg_review_score
    FROM dwh.fact_order;
    """
    df = run_query(sql)
    return df.iloc[0].to_dict()


@st.cache_data(ttl=3600)
def get_monthly_sales_trend(year: Optional[int] = None, state: Optional[str] = None) -> pd.DataFrame:
    """
    Returns monthly sales volume, revenue, freight, and MoM growth.
    Supports optional year and customer state filters.
    """
    where_clauses = ["fo.order_status = 'delivered'"]
    params: Dict[str, Any] = {}

    if year is not None:
        where_clauses.append("d.year = :year")
        params["year"] = int(year)
    if state is not None and state != "All":
        where_clauses.append("c.customer_state = :state")
        params["state"] = state

    where_str = " AND ".join(where_clauses)

    sql = f"""
    WITH monthly_data AS (
        SELECT
            d.year,
            d.month,
            TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
            COUNT(DISTINCT fo.order_id) AS number_of_orders,
            COALESCE(SUM(fo.order_item_count), 0) AS number_of_items,
            ROUND(SUM(fo.total_price), 2) AS gross_product_revenue,
            ROUND(SUM(fo.total_freight), 2) AS freight_revenue,
            ROUND(SUM(fo.total_order_value), 2) AS total_order_value,
            ROUND(AVG(fo.total_order_value), 2) AS average_order_value
        FROM dwh.fact_order fo
        JOIN dwh.dim_date d ON fo.order_date_key = d.date_key
        JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
        WHERE {where_str}
        GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
    )
    SELECT
        year,
        month,
        year_month,
        number_of_orders,
        number_of_items,
        gross_product_revenue,
        freight_revenue,
        total_order_value,
        average_order_value,
        LAG(total_order_value) OVER (ORDER BY year, month) AS prev_month_revenue,
        ROUND(
            (total_order_value - LAG(total_order_value) OVER (ORDER BY year, month))
            / NULLIF(LAG(total_order_value) OVER (ORDER BY year, month), 0) * 100,
            2
        ) AS mom_growth_pct
    FROM monthly_data
    ORDER BY year, month;
    """
    return run_query(sql, params)


@st.cache_data(ttl=3600)
def get_sales_by_state(year: Optional[int] = None) -> pd.DataFrame:
    """Returns aggregated sales and order volume by Brazilian state."""
    where_clause = "WHERE fo.order_status = 'delivered'"
    params: Dict[str, Any] = {}
    if year is not None:
        where_clause += " AND d.year = :year"
        params["year"] = int(year)

    sql = f"""
    SELECT
        c.customer_state,
        COUNT(DISTINCT fo.order_id) AS orders_count,
        COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
        ROUND(SUM(fo.total_price), 2) AS gross_product_revenue,
        ROUND(SUM(fo.total_freight), 2) AS freight_revenue,
        ROUND(SUM(fo.total_order_value), 2) AS total_order_value,
        ROUND(AVG(fo.total_order_value), 2) AS average_order_value,
        ROUND(
            (SUM(fo.total_order_value) / SUM(SUM(fo.total_order_value)) OVER ()) * 100, 
            2
        ) AS revenue_share_pct
    FROM dwh.fact_order fo
    JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
    JOIN dwh.dim_date d ON fo.order_date_key = d.date_key
    {where_clause}
    GROUP BY c.customer_state
    ORDER BY total_order_value DESC;
    """
    return run_query(sql, params)


@st.cache_data(ttl=3600)
def get_top_categories(top_n: int = 10) -> pd.DataFrame:
    """Returns top product categories ranked by revenue with English translation."""
    sql = """
    SELECT
        COALESCE(p.product_category_name_english, p.product_category_name, 'Unknown') AS category_name,
        COUNT(DISTINCT foi.order_id) AS orders_count,
        COUNT(foi.order_item_key) AS units_sold,
        ROUND(SUM(foi.price), 2) AS gross_sales_value,
        ROUND(SUM(foi.freight_value), 2) AS freight_value,
        ROUND(SUM(foi.order_item_total), 2) AS total_category_revenue,
        ROUND(AVG(foi.price), 2) AS average_item_price,
        ROUND(
            (SUM(foi.order_item_total) / SUM(SUM(foi.order_item_total)) OVER ()) * 100, 
            2
        ) AS revenue_contribution_pct
    FROM dwh.fact_order_item foi
    JOIN dwh.dim_product p ON foi.product_key = p.product_key
    GROUP BY COALESCE(p.product_category_name_english, p.product_category_name, 'Unknown')
    ORDER BY total_category_revenue DESC
    LIMIT :top_n;
    """
    return run_query(sql, {"top_n": top_n})


@st.cache_data(ttl=3600)
def get_rfm_segmentation_summary() -> pd.DataFrame:
    """
    Executes customer RFM segmentation in PostgreSQL and aggregates by cohort.
    Descriptive analytical customer segmentation (not machine learning).
    """
    sql = """
    WITH customer_summary AS (
        SELECT
            c.customer_unique_id,
            MAX(d.full_date) AS last_order_date,
            ('2018-10-17'::DATE - MAX(d.full_date)) AS recency_days,
            COUNT(DISTINCT fo.order_id) AS frequency,
            ROUND(SUM(fo.total_order_value), 2) AS monetary
        FROM dwh.fact_order fo
        JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
        JOIN dwh.dim_date d ON fo.order_date_key = d.date_key
        WHERE fo.order_status = 'delivered'
        GROUP BY c.customer_unique_id
    ),
    rfm_scores AS (
        SELECT
            customer_unique_id,
            recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            CASE 
                WHEN frequency >= 3 THEN 5 
                WHEN frequency = 2 THEN 3 
                ELSE 1 
            END AS f_score,
            NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
        FROM customer_summary
    ),
    segmented_customers AS (
        SELECT
            customer_unique_id,
            recency_days,
            frequency,
            monetary,
            CASE
                WHEN r_score >= 4 AND (f_score >= 3 OR m_score = 5) THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score = 1 AND m_score >= 3 THEN 'Potential Loyalists'
                WHEN r_score <= 2 AND (f_score >= 3 OR m_score >= 4) THEN 'At Risk'
                ELSE 'Low Engagement'
            END AS customer_segment
        FROM rfm_scores
    )
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        ROUND((COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER ()) * 100, 2) AS customer_share_pct,
        ROUND(SUM(monetary), 2) AS total_segment_revenue,
        ROUND((SUM(monetary) / SUM(SUM(monetary)) OVER ()) * 100, 2) AS revenue_share_pct,
        ROUND(AVG(monetary), 2) AS avg_monetary_value,
        ROUND(AVG(recency_days), 1) AS avg_recency_days,
        ROUND(AVG(frequency), 2) AS avg_frequency
    FROM segmented_customers
    GROUP BY customer_segment
    ORDER BY total_segment_revenue DESC;
    """
    return run_query(sql)


@st.cache_data(ttl=3600)
def get_sample_rfm_customers(segment: Optional[str] = None, limit: int = 15) -> pd.DataFrame:
    """Returns top customer profiles within a specific RFM segment."""
    where_clause = ""
    params: Dict[str, Any] = {"limit": limit}
    if segment and segment != "All":
        where_clause = "WHERE customer_segment = :segment"
        params["segment"] = segment

    sql = f"""
    WITH customer_summary AS (
        SELECT
            c.customer_unique_id,
            MAX(d.full_date) AS last_order_date,
            ('2018-10-17'::DATE - MAX(d.full_date)) AS recency_days,
            COUNT(DISTINCT fo.order_id) AS frequency,
            ROUND(SUM(fo.total_order_value), 2) AS monetary
        FROM dwh.fact_order fo
        JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
        JOIN dwh.dim_date d ON fo.order_date_key = d.date_key
        WHERE fo.order_status = 'delivered'
        GROUP BY c.customer_unique_id
    ),
    rfm_scores AS (
        SELECT
            customer_unique_id,
            recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            CASE 
                WHEN frequency >= 3 THEN 5 
                WHEN frequency = 2 THEN 3 
                ELSE 1 
            END AS f_score,
            NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
        FROM customer_summary
    ),
    segmented_customers AS (
        SELECT
            customer_unique_id,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            CASE
                WHEN r_score >= 4 AND (f_score >= 3 OR m_score = 5) THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score = 1 AND m_score >= 3 THEN 'Potential Loyalists'
                WHEN r_score <= 2 AND (f_score >= 3 OR m_score >= 4) THEN 'At Risk'
                ELSE 'Low Engagement'
            END AS customer_segment
        FROM rfm_scores
    )
    SELECT
        customer_unique_id,
        customer_segment,
        recency_days,
        frequency,
        monetary AS total_spent,
        r_score,
        f_score,
        m_score
    FROM segmented_customers
    {where_clause}
    ORDER BY monetary DESC, recency_days ASC
    LIMIT :limit;
    """
    return run_query(sql, params)


@st.cache_data(ttl=3600)
def get_logistics_by_state() -> pd.DataFrame:
    """Returns regional delivery performance metrics across all 27 Brazilian states."""
    sql = """
    SELECT
        c.customer_state,
        COUNT(fo.order_id) AS delivered_orders,
        ROUND(AVG(fo.delivery_time_days), 1) AS avg_delivery_days,
        ROUND(AVG(fo.total_freight), 2) AS avg_freight_cost,
        ROUND(
            COUNT(CASE WHEN fo.delivery_delay_days > 0 THEN 1 END)::NUMERIC 
            / COUNT(fo.order_id) * 100, 
            2
        ) AS late_delivery_rate_pct,
        ROUND(AVG(fo.review_score), 2) AS avg_review_score
    FROM dwh.fact_order fo
    JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
    WHERE fo.order_status = 'delivered' 
      AND fo.delivery_time_days IS NOT NULL
    GROUP BY c.customer_state
    ORDER BY avg_delivery_days ASC;
    """
    return run_query(sql)


@st.cache_data(ttl=3600)
def get_delivery_review_correlation() -> pd.DataFrame:
    """Compares review score between on-time and delayed deliveries."""
    sql = """
    SELECT
        CASE 
            WHEN delivery_delay_days > 0 THEN 'Delayed Delivery'
            ELSE 'On-Time Delivery'
        END AS delivery_status,
        COUNT(*) AS order_count,
        ROUND(AVG(delivery_time_days), 1) AS avg_delivery_days,
        ROUND(AVG(review_score), 2) AS avg_customer_rating,
        COUNT(CASE WHEN review_score = 5 THEN 1 END) AS five_star_reviews,
        COUNT(CASE WHEN review_score = 1 THEN 1 END) AS one_star_reviews
    FROM dwh.fact_order
    WHERE order_status = 'delivered' 
      AND delivery_time_days IS NOT NULL 
      AND review_score IS NOT NULL
    GROUP BY CASE WHEN delivery_delay_days > 0 THEN 'Delayed Delivery' ELSE 'On-Time Delivery' END;
    """
    return run_query(sql)


@st.cache_data(ttl=3600)
def get_seller_pareto_analysis() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Calculates seller fulfillment volume, revenue contribution, and cumulative Pareto curve.
    Returns (seller_metrics_df, pareto_summary_dict).
    """
    sql = """
    WITH seller_metrics AS (
        SELECT
            s.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(DISTINCT foi.order_id) AS orders_fulfilled,
            COUNT(foi.order_item_key) AS items_sold,
            ROUND(SUM(foi.order_item_total), 2) AS total_seller_revenue
        FROM dwh.fact_order_item foi
        JOIN dwh.dim_seller s ON foi.seller_key = s.seller_key
        GROUP BY s.seller_id, s.seller_city, s.seller_state
    ),
    ranked_sellers AS (
        SELECT
            seller_id,
            seller_city,
            seller_state,
            orders_fulfilled,
            items_sold,
            total_seller_revenue,
            ROW_NUMBER() OVER (ORDER BY total_seller_revenue DESC) AS seller_rank,
            COUNT(*) OVER () AS total_active_sellers,
            ROUND(
                (SUM(total_seller_revenue) OVER (ORDER BY total_seller_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                / SUM(total_seller_revenue) OVER ()) * 100,
                2
            ) AS cumulative_revenue_pct
        FROM seller_metrics
    )
    SELECT
        seller_rank,
        seller_id,
        seller_city,
        seller_state,
        orders_fulfilled,
        items_sold,
        total_seller_revenue,
        total_active_sellers,
        ROUND((seller_rank::NUMERIC / total_active_sellers) * 100, 2) AS cumulative_seller_pct,
        cumulative_revenue_pct
    FROM ranked_sellers
    ORDER BY seller_rank ASC;
    """
    df = run_query(sql)

    # Compute exact empirical Pareto metrics
    total_active_sellers = int(df["total_active_sellers"].iloc[0]) if not df.empty else 0
    top_20_count = int(round(total_active_sellers * 0.20))
    revenue_from_top_20_pct = float(df.loc[df["seller_rank"] == top_20_count, "cumulative_revenue_pct"].values[0]) if top_20_count <= len(df) else 0.0

    summary = {
        "total_active_sellers": total_active_sellers,
        "top_20_pct_seller_count": top_20_count,
        "revenue_share_of_top_20_pct": revenue_from_top_20_pct,
        "total_marketplace_revenue": float(df["total_seller_revenue"].sum()) if not df.empty else 0.0,
    }
    return df, summary


@st.cache_data(ttl=3600)
def get_pipeline_warehouse_stats() -> Dict[str, Any]:
    """Retrieves verified row counts and audit statistics from PostgreSQL."""
    sql = """
    SELECT
        (SELECT COUNT(*) FROM dwh.dim_date) AS dim_date_rows,
        (SELECT COUNT(*) FROM dwh.dim_customer) AS dim_customer_rows,
        (SELECT COUNT(*) FROM dwh.dim_product) AS dim_product_rows,
        (SELECT COUNT(*) FROM dwh.dim_seller) AS dim_seller_rows,
        (SELECT COUNT(*) FROM dwh.fact_order) AS fact_order_rows,
        (SELECT COUNT(*) FROM dwh.fact_order_item) AS fact_order_item_rows;
    """
    df = run_query(sql)
    counts = df.iloc[0].to_dict()
    counts["total_warehouse_rows"] = sum(counts.values())
    return counts


@st.cache_data(ttl=3600)
def get_filter_options() -> Dict[str, List[Any]]:
    """Returns available years and states for interactive filters."""
    years_df = run_query("""
        SELECT DISTINCT d.year 
        FROM dwh.dim_date d 
        JOIN dwh.fact_order fo ON d.date_key = fo.order_date_key 
        ORDER BY d.year;
    """)
    states_df = run_query("""
        SELECT DISTINCT customer_state 
        FROM dwh.dim_customer 
        WHERE customer_state IS NOT NULL 
        ORDER BY customer_state;
    """)
    return {
        "years": ["All"] + [int(y) for y in years_df["year"].tolist()],
        "states": ["All"] + states_df["customer_state"].tolist(),
    }
