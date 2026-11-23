-- =============================================================================
-- Analytics Query 6: Geographic Sales Performance by Customer State
-- Purpose: Evaluates regional revenue concentration, customer penetration,
--          average order value (AOV), freight burden ratio, and state rankings.
--
-- SQL Concepts: Inner Join, Aggregations, Window Functions (SUM() OVER(), DENSE_RANK),
--               Ratio Calculations, CASE / NULLIF
-- =============================================================================

WITH state_sales AS (
    SELECT
        c.customer_state,
        COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        ROUND(SUM(fo.total_price), 2) AS gross_product_revenue,
        ROUND(SUM(fo.total_freight), 2) AS total_freight_revenue,
        ROUND(SUM(fo.total_order_value), 2) AS total_sales_value,
        ROUND(AVG(fo.total_order_value), 2) AS avg_order_value
    FROM dwh.fact_order fo
    JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
    WHERE fo.order_status = 'delivered'
    GROUP BY c.customer_state
)
SELECT
    customer_state,
    unique_customers,
    total_orders,
    gross_product_revenue,
    total_freight_revenue,
    total_sales_value,
    avg_order_value,
    ROUND(
        (total_freight_revenue / NULLIF(total_sales_value, 0)) * 100,
        2
    ) AS freight_burden_pct,
    ROUND(
        (total_sales_value / SUM(total_sales_value) OVER ()) * 100,
        2
    ) AS national_revenue_share_pct,
    DENSE_RANK() OVER (ORDER BY total_sales_value DESC) AS state_sales_rank
FROM state_sales
ORDER BY state_sales_rank ASC;
