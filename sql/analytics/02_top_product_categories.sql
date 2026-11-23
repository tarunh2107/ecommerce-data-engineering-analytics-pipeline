-- =============================================================================
-- Analytics Query 2: Product Category Performance
-- Purpose: Evaluates merchandise performance across product categories by units sold,
--          distinct customer orders, gross product revenue, freight value,
--          total merchandise value, average item price, and revenue ranking.
--
-- Handling Unlabeled Categories:
--   - COALESCE with 'Unlabeled / Other' ensures null or unmapped category names
--     are transparently aggregated and not silently dropped.
--
-- SQL Concepts: Inner Join, Aggregation (SUM, COUNT, AVG), Window Ranking (DENSE_RANK), COALESCE
-- =============================================================================

SELECT
    COALESCE(p.product_category_name_english, 'Unlabeled / Other') AS category_name,
    COUNT(foi.order_item_key) AS items_sold,
    COUNT(DISTINCT foi.order_id) AS distinct_orders,
    ROUND(SUM(foi.price), 2) AS gross_product_revenue,
    ROUND(SUM(foi.freight_value), 2) AS freight_revenue,
    ROUND(SUM(foi.order_item_total), 2) AS total_item_value,
    ROUND(AVG(foi.price), 2) AS average_item_price,
    DENSE_RANK() OVER (ORDER BY SUM(foi.order_item_total) DESC) AS revenue_rank
FROM dwh.fact_order_item foi
JOIN dwh.dim_product p ON foi.product_key = p.product_key
GROUP BY COALESCE(p.product_category_name_english, 'Unlabeled / Other')
ORDER BY revenue_rank ASC;
