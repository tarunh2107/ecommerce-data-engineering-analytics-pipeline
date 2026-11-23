-- =============================================================================
-- Analytics Query 4: Customer RFM (Recency, Frequency, Monetary) Segmentation
-- Purpose: Builds an interpretable customer segmentation model evaluating:
--          - Recency  : Days elapsed since customer's last purchase (relative to snapshot date 2018-10-17).
--          - Frequency: Total distinct delivered orders placed by customer_unique_id.
--          - Monetary : Total gross merchandise value spent (products + freight).
--
-- Note: This is descriptive analytical SQL segmentation, not predictive machine learning.
--
-- Customer Cohort Definitions:
--   - Champions          : High recent engagement with top-tier monetary spend or repeat orders.
--   - Loyal Customers    : Frequent repeat buyers across medium to recent engagement windows.
--   - Potential Loyalists: Recent buyers with above-average first-time basket spend.
--   - At Risk            : Valuable or repeat customers who have not ordered recently.
--   - Low Engagement     : Low-spend, single-order customers with high recency inactivity.
--
-- SQL Concepts: Multi-stage CTEs, Window NTILE, CASE expressions, Aggregations, Date Arithmetic
-- =============================================================================

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
        -- Recency: 5 = Most recent (lowest recency_days), 1 = Least recent
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        -- Frequency: 5 = 3+ orders, 3 = 2 orders, 1 = 1 order
        CASE 
            WHEN frequency >= 3 THEN 5 
            WHEN frequency = 2 THEN 3 
            ELSE 1 
        END AS f_score,
        -- Monetary: 5 = Highest spenders, 1 = Lowest spenders
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM customer_summary
)
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
ORDER BY monetary DESC, recency_days ASC;
