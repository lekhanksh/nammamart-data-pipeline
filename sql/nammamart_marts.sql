CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.is_current AND o.dq_status <> 'FAIL'
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS revenue,
    ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAIL'
WHERE o.is_current AND o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    ROUND(AVG(o.delivery_minutes), 2) AS average_delivery_minutes,
    COUNT(*) AS delivered_orders,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0), 2)
        AS within_15_minutes_pct
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.is_current AND o.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED' AND o.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (
    SELECT order_id, order_status, order_amount, payment_mode
    FROM stg_orders
    WHERE is_current AND dq_status <> 'FAIL'
),
payments AS (
    SELECT payment_id, order_id, amount_paid, payment_mode, payment_status, dq_status
    FROM stg_payments
    WHERE is_current
)
SELECT
    COALESCE(o.order_id, p.order_id) AS order_id,
    p.payment_id,
    o.order_status,
    p.payment_status,
    o.order_amount,
    p.amount_paid,
    ROUND(COALESCE(o.order_amount, 0) - COALESCE(p.amount_paid, 0), 2) AS discrepancy_amount,
    CASE
        WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT'
        WHEN p.payment_id IS NULL AND o.order_status = 'DELIVERED' THEN 'DELIVERED_ORDER_MISSING_PAYMENT'
        WHEN o.order_status = 'DELIVERED' AND p.payment_status <> 'SUCCESS' THEN 'DELIVERED_ORDER_PAYMENT_NOT_SUCCESS'
        WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN 'CANCELLED_ORDER_CHARGED'
        WHEN o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS' THEN 'RETURNED_ORDER_NOT_REFUNDED'
        WHEN p.payment_status = 'SUCCESS' AND ABS(o.order_amount - p.amount_paid) > 0.05
            THEN 'SUCCESS_PAYMENT_AMOUNT_MISMATCH'
        WHEN p.dq_status = 'FAIL' THEN 'INVALID_PAYMENT_RECORD'
        ELSE 'RECONCILED'
    END AS mismatch_reason
FROM orders o
FULL OUTER JOIN payments p ON o.order_id = p.order_id
WHERE o.order_id IS NULL
   OR (p.payment_id IS NULL AND o.order_status = 'DELIVERED')
   OR p.dq_status = 'FAIL'
   OR (o.order_status = 'DELIVERED' AND p.payment_status <> 'SUCCESS')
   OR (o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS')
   OR (o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS')
   OR (p.payment_status = 'SUCCESS' AND ABS(o.order_amount - p.amount_paid) > 0.05);

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.loyalty_tier,
    ROUND(SUM(o.order_amount), 2) AS total_spend,
    RANK() OVER (ORDER BY SUM(o.order_amount) DESC) AS spend_rank
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id
WHERE o.is_current AND o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
  AND c.is_current AND c.dq_status <> 'FAIL'
GROUP BY o.customer_id, c.loyalty_tier
QUALIFY spend_rank <= 10;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT
    payment_mode,
    ROUND(SUM(order_amount), 2) AS net_revenue
FROM stg_orders
WHERE is_current AND dq_status <> 'FAIL' AND order_status = 'DELIVERED'
GROUP BY payment_mode;

CREATE OR REPLACE TABLE mart_rejections AS
SELECT
    _source_file AS source_file,
    order_id AS row_key,
    failure_reason
FROM stg_orders
WHERE dq_status = 'FAIL'
UNION ALL
SELECT
    'customers.csv',
    customer_id,
    failure_reason
FROM stg_customers
WHERE dq_status = 'FAIL'
UNION ALL
SELECT
    'products.csv',
    product_id,
    failure_reason
FROM stg_products
WHERE dq_status = 'FAIL'
UNION ALL
SELECT
    'payments.csv',
    payment_id,
    failure_reason
FROM stg_payments
WHERE dq_status = 'FAIL';
