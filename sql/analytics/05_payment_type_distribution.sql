-- =============================================================================
-- Analytics Query 5: Order Payment Value Tiers & Checkout Ticket Distribution
-- Purpose: Analyzes transaction value brackets, order volume concentration,
--          monetary share, average items per order, and customer satisfaction.
--
-- Architectural Note on Payment Types:
--   The Kimball Star Schema (dwh) intentionally stores total_payment at the
--   fact_order level (Grain: One row per order). Because customers can split payments
--   across multiple payment types (credit card, voucher, boleto), analyzing payment_type
--   at the transaction line level requires a separate transaction fact table (dwh.fact_order_payment).
--   To preserve warehouse grain integrity and avoid unverified joins against raw staging,
--   this query evaluates order-level payment tier distribution supported natively by dwh.fact_order.
--
-- SQL Concepts: CTE, CASE Expressions, Window Aggregations (SUM() OVER ()), GROUP BY, Filtering
-- =============================================================================

WITH payment_tiers AS (
    SELECT
        fo.order_key,
        fo.order_id,
        fo.total_payment,
        fo.order_item_count,
        fo.review_score,
        CASE
            WHEN fo.total_payment < 50.00 THEN '1. Under R$ 50 (Micro)'
            WHEN fo.total_payment >= 50.00 AND fo.total_payment < 100.00 THEN '2. R$ 50 - R$ 100 (Low)'
            WHEN fo.total_payment >= 100.00 AND fo.total_payment < 250.00 THEN '3. R$ 100 - R$ 250 (Mid)'
            WHEN fo.total_payment >= 250.00 AND fo.total_payment < 500.00 THEN '4. R$ 250 - R$ 500 (High)'
            WHEN fo.total_payment >= 500.00 AND fo.total_payment < 1000.00 THEN '5. R$ 500 - R$ 1,000 (Premium)'
            ELSE '6. R$ 1,000+ (VIP / Bulk)'
        END AS payment_tier,
        CASE
            WHEN fo.total_payment < 50.00 THEN 1
            WHEN fo.total_payment >= 50.00 AND fo.total_payment < 100.00 THEN 2
            WHEN fo.total_payment >= 100.00 AND fo.total_payment < 250.00 THEN 3
            WHEN fo.total_payment >= 250.00 AND fo.total_payment < 500.00 THEN 4
            WHEN fo.total_payment >= 500.00 AND fo.total_payment < 1000.00 THEN 5
            ELSE 6
        END AS tier_sort_order
    FROM dwh.fact_order fo
    WHERE fo.order_status != 'canceled'
)
SELECT
    payment_tier,
    COUNT(order_key) AS total_orders,
    ROUND(SUM(total_payment), 2) AS total_payment_value,
    ROUND(AVG(total_payment), 2) AS avg_payment_value,
    ROUND(AVG(order_item_count), 2) AS avg_order_items,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    ROUND(
        COUNT(order_key)::NUMERIC 
        / SUM(COUNT(order_key)) OVER () * 100, 
        2
    ) AS order_share_pct,
    ROUND(
        SUM(total_payment)::NUMERIC 
        / SUM(SUM(total_payment)) OVER () * 100, 
        2
    ) AS revenue_share_pct
FROM payment_tiers
GROUP BY payment_tier, tier_sort_order
ORDER BY tier_sort_order ASC;
