CREATE OR REPLACE TABLE stg_customers AS
WITH latest_source AS (
    SELECT * EXCLUDE(source_run_rank)
    FROM (
        SELECT *, DENSE_RANK() OVER (
            PARTITION BY _source_file ORDER BY _loaded_at DESC
        ) AS source_run_rank
        FROM raw_customers
    )
    WHERE source_run_rank = 1
),
typed AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id ORDER BY _source_row
           ) AS key_rank,
           COUNT(*) OVER (PARTITION BY customer_id) AS key_count
    FROM latest_source
)
SELECT
    customer_id,
    CASE
        WHEN NULLIF(TRIM(full_name), '') IS NULL THEN 'unknown'
        ELSE LEFT(TRIM(full_name), 1) || '***'
    END AS full_name,
    CASE
        WHEN email IS NULL OR TRIM(email) = '' OR POSITION('@' IN TRIM(email)) = 0
            THEN 'invalid_email'
        ELSE LEFT(TRIM(email), 2) || '***@' || SPLIT_PART(TRIM(email), '@', 2)
    END AS email,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 10
            THEN LEFT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2)
                 || '******' || RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2)
        ELSE 'invalid_phone'
    END AS phone,
    COALESCE(NULLIF(TRIM(city), ''), 'Bengaluru') AS city,
    'Restricted' AS area,
    CAST(NULL AS INTEGER) AS pincode,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    UPPER(LEFT(TRIM(loyalty_tier), 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) AS loyalty_tier,
    LOWER(TRIM(is_active)) IN ('true', '1', 'yes') AS is_active,
    key_rank = 1 AS is_current,
    CASE
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'FAIL'
        WHEN key_rank > 1 THEN 'FAIL'
        WHEN key_count > 1 OR POSITION('@' IN COALESCE(email, '')) = 0
             OR LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) <> 10
             OR TRY_CAST(signup_date AS DATE) > DATE '2026-10-01'
             OR loyalty_tier <> (
                 UPPER(LEFT(TRIM(loyalty_tier), 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2))
             )
            THEN 'WARN'
        ELSE 'PASS'
    END AS dq_status,
    CONCAT_WS(
        ';',
        CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'MISSING_CUSTOMER_ID' END,
        CASE WHEN key_rank > 1 THEN 'DUPLICATE_CUSTOMER_ID' END,
        CASE WHEN key_rank = 1 AND key_count > 1 THEN 'DUPLICATE_CUSTOMER_ID_DEDUPED' END,
        CASE WHEN POSITION('@' IN COALESCE(email, '')) = 0 THEN 'INVALID_EMAIL' END,
        CASE WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) <> 10 THEN 'INVALID_PHONE' END,
        CASE WHEN TRY_CAST(signup_date AS DATE) > DATE '2026-10-01' THEN 'FUTURE_SIGNUP_DATE' END,
        CASE WHEN loyalty_tier <> (
            UPPER(LEFT(TRIM(loyalty_tier), 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2))
        ) THEN 'LOYALTY_TIER_CASE_STANDARDIZED' END
    ) AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_products AS
WITH latest_source AS (
    SELECT * EXCLUDE(source_run_rank)
    FROM (
        SELECT *, DENSE_RANK() OVER (
            PARTITION BY _source_file ORDER BY _loaded_at DESC
        ) AS source_run_rank
        FROM raw_products
    )
    WHERE source_run_rank = 1
),
typed AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _source_row) AS key_rank,
           COUNT(*) OVER (PARTITION BY product_id) AS key_count
    FROM latest_source
)
SELECT
    product_id,
    TRIM(product_name) AS product_name,
    NULLIF(TRIM(category), '') AS category,
    sub_category,
    brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp,
    TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    unit,
    LOWER(TRIM(is_perishable)) IN ('true', '1', 'yes') AS is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date,
    key_rank = 1 AS is_current,
    CASE
        WHEN product_id IS NULL OR TRIM(product_id) = '' OR key_rank > 1
             OR NULLIF(TRIM(category), '') IS NULL
             OR TRY_CAST(mrp AS DOUBLE) <= 0
             OR TRY_CAST(cost_price AS DOUBLE) <= 0
             OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE)
            THEN 'FAIL'
        WHEN key_count > 1 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_status,
    CONCAT_WS(
        ';',
        CASE WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'MISSING_PRODUCT_ID' END,
        CASE WHEN key_rank > 1 THEN 'DUPLICATE_PRODUCT_ID' END,
        CASE WHEN key_rank = 1 AND key_count > 1 THEN 'DUPLICATE_PRODUCT_ID_DEDUPED' END,
        CASE WHEN NULLIF(TRIM(category), '') IS NULL THEN 'MISSING_CATEGORY' END,
        CASE WHEN TRY_CAST(mrp AS DOUBLE) <= 0 OR TRY_CAST(cost_price AS DOUBLE) <= 0 THEN 'NON_POSITIVE_PRICE' END,
        CASE WHEN TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'COST_GREATER_THAN_MRP' END
    ) AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_stores AS
WITH latest_source AS (
    SELECT * EXCLUDE(source_run_rank)
    FROM (
        SELECT *, DENSE_RANK() OVER (
            PARTITION BY _source_file ORDER BY _loaded_at DESC
        ) AS source_run_rank
        FROM raw_stores
    )
    WHERE source_run_rank = 1
)
SELECT
    TRIM(store_id) AS store_id,
    TRIM(store_name) AS store_name,
    COALESCE(NULLIF(UPPER(LEFT(TRIM(zone), 1)) || LOWER(SUBSTR(TRIM(zone), 2)), ''), 'South') AS zone,
    area,
    TRY_CAST(pincode AS INTEGER) AS pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    CASE
        WHEN NULLIF(TRIM(manager_name), '') IS NULL THEN 'unknown'
        ELSE LEFT(TRIM(manager_name), 1) || '***'
    END AS manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN zone IS NULL OR TRIM(zone) = '' OR TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0
         THEN 'WARN' ELSE 'PASS' END AS dq_status,
    CONCAT_WS(
        ';',
        CASE WHEN zone IS NULL OR TRIM(zone) = '' THEN 'MISSING_ZONE_IMPUTED_SOUTH' END,
        CASE WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'NON_POSITIVE_STORE_CAPACITY' END
    ) AS failure_reason
FROM latest_source;

CREATE OR REPLACE TABLE stg_orders AS
WITH latest_source AS (
    SELECT * EXCLUDE(source_run_rank)
    FROM (
        SELECT *, DENSE_RANK() OVER (
            PARTITION BY _source_file ORDER BY _loaded_at DESC
        ) AS source_run_rank
        FROM raw_orders
    )
    WHERE source_run_rank = 1
),
numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY CASE WHEN _source_file LIKE '%_increment.csv' THEN 1 ELSE 0 END DESC,
                        _loaded_at DESC, _source_row
           ) AS key_rank,
           COUNT(*) OVER (PARTITION BY order_id, _source_file) AS file_key_count,
           MAX(CASE WHEN _source_file LIKE '%_increment.csv' THEN 1 ELSE 0 END)
               OVER (PARTITION BY order_id) AS has_increment
    FROM latest_source
),
typed AS (
    SELECT *,
           TRY_CAST(order_ts AS TIMESTAMP) AS parsed_order_ts,
           TRY_CAST(quantity AS INTEGER) AS parsed_quantity,
           TRY_CAST(unit_price AS DOUBLE) AS parsed_unit_price,
           TRY_CAST(discount_pct AS DOUBLE) AS parsed_discount_pct,
           TRY_CAST(order_amount AS DOUBLE) AS parsed_order_amount,
           TRY_CAST(delivery_minutes AS DOUBLE) AS parsed_delivery_minutes,
           UPPER(TRIM(order_status)) AS normalized_status,
           CASE LOWER(TRIM(payment_mode))
               WHEN 'upi' THEN 'UPI'
               WHEN 'card' THEN 'Card'
               WHEN 'cash on delivery' THEN 'Cash on Delivery'
               WHEN 'cod' THEN 'Cash on Delivery'
               WHEN 'wallet' THEN 'Wallet'
               ELSE TRIM(payment_mode)
           END AS normalized_payment_mode
    FROM numbered
),
checked AS (
    SELECT t.*,
           CONCAT_WS(
               ';',
               CASE WHEN key_rank > 1 AND has_increment = 0 THEN 'DUPLICATE_ORDER_ID' END,
               CASE WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'MISSING_ORDER_ID' END,
               CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'MISSING_CUSTOMER_ID' END,
               CASE WHEN parsed_order_ts IS NULL THEN 'INVALID_ORDER_TS' END,
               CASE WHEN parsed_order_ts > TIMESTAMP '2026-10-01 23:59:59' THEN 'FUTURE_ORDER_TS' END,
               CASE WHEN normalized_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'INVALID_ORDER_STATUS' END,
               CASE WHEN normalized_payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'INVALID_PAYMENT_MODE' END,
               CASE WHEN parsed_quantity IS NULL OR parsed_quantity <= 0 THEN 'NON_POSITIVE_QUANTITY' END,
               CASE WHEN parsed_discount_pct IS NULL OR parsed_discount_pct < 0 OR parsed_discount_pct > 100 THEN 'DISCOUNT_OUT_OF_RANGE' END,
               CASE WHEN parsed_order_amount IS NULL OR ABS(parsed_order_amount -
                    ROUND(parsed_quantity * parsed_unit_price * (1 - parsed_discount_pct / 100), 2)) > 0.05
                    THEN 'ORDER_AMOUNT_FORMULA_MISMATCH' END,
               CASE WHEN normalized_status = 'DELIVERED' AND parsed_delivery_minutes IS NULL THEN 'DELIVERED_MISSING_MINUTES' END,
               CASE WHEN normalized_status = 'DELIVERED' AND parsed_delivery_minutes IS NOT NULL
                         AND (parsed_delivery_minutes <= 0 OR parsed_delivery_minutes >= 120)
                    THEN 'DELIVERY_MINUTES_OUT_OF_BOUNDS' END,
               CASE WHEN customer_id IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM stg_customers c WHERE c.customer_id = TRIM(t.customer_id)
                      AND c.is_current AND c.dq_status <> 'FAIL'
               ) THEN 'ORPHAN_CUSTOMER_ID' END,
               CASE WHEN NOT EXISTS (
                    SELECT 1 FROM stg_products p WHERE p.product_id = TRIM(t.product_id)
                      AND p.is_current AND p.dq_status <> 'FAIL'
               ) THEN 'ORPHAN_OR_INVALID_PRODUCT_ID' END,
               CASE WHEN NOT EXISTS (
                    SELECT 1 FROM stg_stores s WHERE s.store_id = TRIM(t.store_id)
               ) THEN 'ORPHAN_STORE_ID' END,
               CASE WHEN key_rank > 1 AND has_increment = 1
                         AND _source_file NOT LIKE '%_increment.csv' THEN 'SUPERSEDED_BY_INCREMENT' END
           ) AS failure_reason
    FROM typed t
)
SELECT
    TRIM(order_id) AS order_id,
    parsed_order_ts AS order_ts,
    TRIM(customer_id) AS customer_id,
    TRIM(store_id) AS store_id,
    TRIM(product_id) AS product_id,
    parsed_quantity AS quantity,
    parsed_unit_price AS unit_price,
    parsed_discount_pct AS discount_pct,
    parsed_order_amount AS order_amount,
    normalized_payment_mode AS payment_mode,
    normalized_status AS order_status,
    channel,
    parsed_delivery_minutes AS delivery_minutes,
    _source_file,
    _run_id,
    key_rank = 1 AS is_current,
    CASE
        WHEN key_rank > 1 AND has_increment = 1 AND _source_file NOT LIKE '%_increment.csv' THEN 'WARN'
        WHEN failure_reason <> '' THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    failure_reason
FROM checked;

CREATE OR REPLACE TABLE stg_payments AS
WITH latest_source AS (
    SELECT * EXCLUDE(source_run_rank)
    FROM (
        SELECT *, DENSE_RANK() OVER (
            PARTITION BY _source_file ORDER BY _loaded_at DESC
        ) AS source_run_rank
        FROM raw_payments
    )
    WHERE source_run_rank = 1
),
typed AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _source_row) AS key_rank,
           COUNT(*) OVER (PARTITION BY payment_id) AS key_count,
           CASE LOWER(TRIM(payment_mode))
               WHEN 'upi' THEN 'UPI'
               WHEN 'card' THEN 'Card'
               WHEN 'cash on delivery' THEN 'Cash on Delivery'
               WHEN 'cod' THEN 'Cash on Delivery'
               WHEN 'wallet' THEN 'Wallet'
               ELSE TRIM(payment_mode)
           END AS normalized_payment_mode
    FROM latest_source
)
SELECT
    TRIM(payment_id) AS payment_id,
    TRIM(order_id) AS order_id,
    TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
    normalized_payment_mode AS payment_mode,
    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(payment_status)) AS payment_status,
    gateway_ref,
    key_rank = 1 AS is_current,
    CASE
        WHEN payment_id IS NULL OR TRIM(payment_id) = '' OR key_rank > 1
             OR normalized_payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
             OR TRY_CAST(amount_paid AS DOUBLE) IS NULL OR TRY_CAST(amount_paid AS DOUBLE) <= 0
            THEN 'FAIL'
        WHEN key_count > 1 THEN 'WARN'
        WHEN UPPER(TRIM(payment_status)) NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'WARN'
        ELSE 'PASS'
    END AS dq_status,
    CONCAT_WS(
        ';',
        CASE WHEN payment_id IS NULL OR TRIM(payment_id) = '' THEN 'MISSING_PAYMENT_ID' END,
        CASE WHEN key_rank > 1 THEN 'DUPLICATE_PAYMENT_ID' END,
        CASE WHEN key_rank = 1 AND key_count > 1 THEN 'DUPLICATE_PAYMENT_ID_DEDUPED' END,
        CASE WHEN normalized_payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'INVALID_PAYMENT_MODE' END,
        CASE WHEN TRY_CAST(amount_paid AS DOUBLE) IS NULL OR TRY_CAST(amount_paid AS DOUBLE) <= 0 THEN 'NON_POSITIVE_AMOUNT_PAID' END,
        CASE WHEN UPPER(TRIM(payment_status)) NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'INVALID_PAYMENT_STATUS' END
    ) AS failure_reason
FROM typed;

INSERT INTO dq_results
WITH checks AS (
    SELECT 'orders_required_customer' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE customer_id IS NULL OR TRIM(customer_id) = '') AS rows_failed
    FROM stg_orders WHERE is_current
    UNION ALL
    SELECT 'orders_unique_id', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%DUPLICATE_ORDER_ID%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_allowed_values', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%INVALID_ORDER_STATUS%'
                             OR failure_reason LIKE '%INVALID_PAYMENT_MODE%')
    FROM stg_orders WHERE is_current
    UNION ALL
    SELECT 'orders_amount_and_delivery', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%FORMULA_MISMATCH%'
                             OR failure_reason LIKE '%OUT_OF_BOUNDS%')
    FROM stg_orders WHERE is_current
    UNION ALL
    SELECT 'orders_foreign_keys', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%ORPHAN_%')
    FROM stg_orders WHERE is_current
    UNION ALL
    SELECT 'orders_timestamp_current', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%ORDER_TS%')
    FROM stg_orders WHERE is_current
    UNION ALL
    SELECT 'orders_volume_baseline', 'Completeness', 'Warning', COUNT(*),
           CASE WHEN COUNT(*) < 242 THEN 1 ELSE 0 END
    FROM stg_orders WHERE is_current AND dq_status <> 'FAIL'
    UNION ALL
    SELECT 'orders_freshness_1d', 'Timeliness', 'Warning', COUNT(*),
           CASE WHEN MAX(order_ts) < CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 ELSE 0 END
    FROM stg_orders WHERE is_current AND dq_status <> 'FAIL'
    UNION ALL
    SELECT 'customers_unique_id', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%DUPLICATE_CUSTOMER_ID%')
    FROM stg_customers
    UNION ALL
    SELECT 'products_valid_price_category', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE dq_status = 'FAIL')
    FROM stg_products
    UNION ALL
    SELECT 'payments_positive_and_unique', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE dq_status = 'FAIL')
    FROM stg_payments
    UNION ALL
    SELECT 'customer_contact_format', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE email = 'invalid_email' OR phone = 'invalid_phone')
    FROM stg_customers
    UNION ALL
    SELECT 'customer_loyalty_tier_casing', 'Consistency', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%LOYALTY_TIER_CASE_STANDARDIZED%')
    FROM stg_customers
    UNION ALL
    SELECT 'store_capacity_positive', 'Accuracy', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE capacity_orders_per_day <= 0)
    FROM stg_stores
    UNION ALL
    SELECT 'payment_order_exists', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE NOT EXISTS (
               SELECT 1 FROM stg_orders o
               WHERE o.order_id = stg_payments.order_id AND o.is_current
           ))
    FROM stg_payments WHERE is_current
)
SELECT '{{RUN_ID}}', 'nammamart_elt_lekhanksh', check_name, dimension, severity,
       rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS'
            WHEN severity = 'Warning' THEN 'WARN' ELSE 'FAIL' END,
       CURRENT_TIMESTAMP
FROM checks;
