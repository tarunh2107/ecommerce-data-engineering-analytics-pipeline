-- =============================================================================
-- Analytics Query 3: Delivery Performance & Logistics SLA by Customer State
-- Purpose: Analyzes logistics efficiency, delivery duration, delayed shipments,
--          delay rate percentages, and customer satisfaction across destination states.
--
-- Business Rules:
--   - Filtered to delivered orders (order_status = 'delivered' and delivery_time_days IS NOT NULL).
--   - Excludes canceled/unavailable/created orders from SLA delay calculations.
--   - is_late_delivery = TRUE indicates actual delivery exceeded estimated delivery date.
--   - avg_delay_days computes average days delayed specifically for late orders.
--
-- SQL Concepts: Inner Join, Conditional Aggregation (CASE / FILTER), Window Ranking (RANK), ROUND
-- =============================================================================

SELECT
    c.customer_state,
    COUNT(fo.order_key) AS delivered_orders,
    ROUND(AVG(fo.delivery_time_days)::NUMERIC, 2) AS avg_delivery_time_days,
    COUNT(*) FILTER (WHERE fo.is_late_delivery = TRUE) AS late_deliveries,
    ROUND(
        COUNT(*) FILTER (WHERE fo.is_late_delivery = TRUE)::NUMERIC
        / NULLIF(COUNT(fo.order_key), 0) * 100,
        2
    ) AS late_delivery_rate_pct,
    ROUND(
        AVG(fo.delivery_delay_days) FILTER (WHERE fo.is_late_delivery = TRUE)::NUMERIC,
        2
    ) AS avg_delay_days,
    ROUND(AVG(fo.review_score)::NUMERIC, 2) AS avg_review_score,
    RANK() OVER (ORDER BY AVG(fo.delivery_time_days) ASC) AS delivery_speed_rank
FROM dwh.fact_order fo
JOIN dwh.dim_customer c ON fo.customer_key = c.customer_key
WHERE fo.order_status = 'delivered'
  AND fo.delivery_time_days IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_time_days ASC;
