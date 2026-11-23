-- =============================================================================
-- Analytics Query 7: Merchant & Seller Fulfillment Performance (Pareto Analysis)
-- Purpose: Evaluates merchant fulfillment volume, revenue contribution,
--          average unit prices, seller ranking, and cumulative revenue concentration (Pareto 80/20).
--
-- SQL Concepts: Inner Join, Aggregations, Window Functions (DENSE_RANK, Window Running Sum),
--               Cumulative Percentage Calculation, Division by Zero Protection
-- =============================================================================

WITH seller_metrics AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT foi.order_id) AS orders_fulfilled,
        COUNT(foi.order_item_key) AS items_sold,
        ROUND(SUM(foi.price), 2) AS gross_product_revenue,
        ROUND(SUM(foi.freight_value), 2) AS freight_generated,
        ROUND(SUM(foi.order_item_total), 2) AS total_seller_revenue,
        ROUND(AVG(foi.price), 2) AS avg_item_price,
        ROUND(
            COUNT(foi.order_item_key)::NUMERIC 
            / NULLIF(COUNT(DISTINCT foi.order_id), 0), 
            2
        ) AS avg_items_per_order
    FROM dwh.fact_order_item foi
    JOIN dwh.dim_seller s ON foi.seller_key = s.seller_key
    GROUP BY s.seller_id, s.seller_city, s.seller_state
)
SELECT
    seller_id,
    seller_city,
    seller_state,
    orders_fulfilled,
    items_sold,
    gross_product_revenue,
    freight_generated,
    total_seller_revenue,
    avg_item_price,
    avg_items_per_order,
    DENSE_RANK() OVER (ORDER BY total_seller_revenue DESC) AS seller_rank,
    ROUND(
        (SUM(total_seller_revenue) OVER (ORDER BY total_seller_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / SUM(total_seller_revenue) OVER ()) * 100,
        2
    ) AS cumulative_revenue_share_pct
FROM seller_metrics
ORDER BY seller_rank ASC;
