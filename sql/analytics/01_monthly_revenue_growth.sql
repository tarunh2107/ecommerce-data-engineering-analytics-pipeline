-- =============================================================================
-- Analytics Query 1: Monthly Sales & Revenue Growth Analysis
-- Purpose: Tracks monthly order volume, units sold, gross product revenue,
--          freight revenue, total order value (GMV), average order value (AOV),
--          and Month-over-Month (MoM) revenue growth percentage.
--
-- Revenue Definitions:
--   - Gross Product Revenue: SUM(fo.total_price) representing item sales merchandise value.
--   - Freight Revenue      : SUM(fo.total_freight) representing logistics charges paid.
--   - Total Order Value    : SUM(fo.total_order_value) = Product Revenue + Freight Revenue.
--   - Recognized Scope     : Delivered orders (delivered customer fulfillment).
--
-- SQL Concepts: CTE, Joins, Window Functions (LAG), Date Aggregation, NULLIF, ROUND
-- =============================================================================

WITH monthly_sales AS (
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
    WHERE fo.order_status = 'delivered'
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
FROM monthly_sales
ORDER BY year, month;
