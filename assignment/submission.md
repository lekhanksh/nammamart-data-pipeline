# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

## Implementation and test status — 22 October 2026

Both owner-specific DAGs are implemented and `astro dev parse` reports no import errors. Each DAG was run repeatedly with the supplied six source files. ETL uses Python/pandas before loading its star schema; ELT appends raw CSV rows as text, then runs DuckDB SQL from `include/sql/` to build staging and marts.

| Requirement | Status |
| :--- | :--- |
| Part 0 — profile and issues log | Profile script covers all six files, date/numeric ranges, category values, referential integrity, and business-rule checks; log and client questions are below. |
| Part A — ETL | Implemented; DAG test succeeded. Fact/dimension, reconciliation, quality, quarantine, reports, and run-audit outputs exist. Repeat-run counts were stable. |
| Part B — ELT | Implemented; DAG test succeeded. Raw, staging, and six mart/rejection tables were built from SQL. Raw is intentionally append-only; staging picks the latest source snapshot and upserts orders by `order_id`. |
| Part C — schedules | Both DAGs declare `0 9 * * 1-6` and `Asia/Kolkata`, `catchup=False`, retries, retry delay, and timeout. Both are now unpaused. Airflow created the previous logical interval late after the scheduler was restored; its successful recovery does not establish that the 09:00 IST SLA was met. |
| Part D — quality dimensions | Both pipelines record quality checks for completeness, uniqueness, validity, accuracy, consistency, and timeliness in `dq_results`. |
| Part E — governance | Answers and a compact dictionary are below. |
| Part F — observability | Run audit, DQ scorecards, volume/freshness warnings, row-count/re-run checks, and incremental behavior are implemented. Not all ten destructive-input experiments were run; the matrix below separates observations from unexecuted tests. |
| Part G — recommendation | Evidence-based recommendation is below. |

### Verified local run evidence

`astro dev parse` passed (4 DAG-integrity tests passed). Both DAGs' late scheduled run for logical interval `2026-10-21 09:00 IST` completed with all tasks and audit rows in `SUCCESS`: ETL extracted 5,372 / loaded 2,886 / quarantined 267; ELT extracted 5,372 / loaded 2,329 / quarantined 264. The loaded fact data matches across pipelines: **2,329 valid current orders** and **1,139,073.20** delivered net revenue after the increment updates. The September subset is **2,192 orders** and **1,066,513.00** delivered revenue. ETL's daily zone/store table sums to the same all-date revenue.

Airflow evidence: [dashboard status](./airflow_dashboard.png), [recovered ELT scheduled run](./airflow_elt_recovery.png), and the existing [ETL-versus-ELT comparison](./etl_vs_elt.png).

The test runs included the 160-row incremental file: **150 new IDs** and **10 existing IDs changed to `RETURNED`**. ELT raw ingestion is append-only; a guard now prevents retrying the same file twice within one run. One earlier test did append the order sources twice under one run ID (7,944 extracted instead of 5,372); its raw records were intentionally not deleted, and the current staged facts/marts remain deduplicated and stable. The local warehouse is `include/warehouse_lekhanksh.duckdb`; it and generated customer-bearing quarantine files are git-ignored and should not be committed.

## Part 0: Discovery and Data Profiling

### 1. File Profile Summary
| File | Total Rows | Null Columns | Duplicate Primary Keys | Numeric / Date Anomalies | Category / Format Issues |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `customers.csv` | 405 | `email` (4), `city` (6) | 5 keys (`C00103`, `C00252`, `C00036`, `C00226`, `C00313`) | `phone` min `9804341` (not 10 digits); 3 future `signup_date`s (`>2026-10-01`) | `loyalty_tier` mixed casing (`SILVER`, `Silver`, `BRONZE`, `bronze`) |
| `products.csv` | 152 | `category` (3) | 2 keys (`P0030`, `P0140`) | `mrp` min `-203.0`; 5 rows where `cost_price > mrp` | 3 products with `NaN` category |
| `stores.csv` | 15 | `zone` (1) | 0 | `capacity_orders_per_day` min `0` (`S12`) | `S07` (BTM Layout) has `NaN` zone |
| `orders.csv` | 2,412 | `order_ts` (2), `customer_id` (18), `delivery_minutes` (164) | 12 keys (e.g., `ORD001462`, `ORD001326`, `ORD001434`) | `quantity` min `-3`; `discount_pct` max `250`; `order_amount` min `-1593.0`; `delivery_minutes` max `1440.0`; 13 future timestamps and 2 null timestamps | `order_status` has `'delivered'`, `'Pending??'`; `payment_mode` has `'Cashh'`, `'upi'`, `'UPI '`, `'cod'`, `'CARD'` |
| `payments.csv` | 2,228 | `gateway_ref` (301) | 4 keys (`PAY0001651`, `PAY0000755`, `PAY0000509`, `PAY0002000`) | `amount_paid` min `-1593.0` | `payment_mode` has `'upi'`, `'cod'`, `'CARD'`, `'UPI '`, `'Cashh'` |
| `orders_2026-10-01_increment.csv` | 160 | None | 0 | All within valid bounds (`quantity` 1–4, `discount_pct` 0–20, `delivery_minutes` 8–34) | Contains 150 new Oct 1 orders + 10 status updates (`RETURNED`) for Sept orders |

#### Numeric/date ranges and category values

Ranges below include every numeric/date-typed column found by the profiling script; the order timestamps include invalid/null values separately as noted above.

| File | Numeric min–max | Date min–max |
| :--- | :--- | :--- |
| `customers.csv` | `phone`: 9,804,341–9,999,393,305 | `signup_date`: 2024-06-01–2027-01-05 |
| `products.csv` | `mrp`: -203–883; `cost_price`: 6.79–631.79 | `launch_date`: 2023-01-03–2025-09-20 |
| `stores.csv` | `pincode`: 560003–560102; `capacity_orders_per_day`: 0–600 | `open_date`: 2024-02-14–2025-07-28 |
| `orders.csv` | `quantity`: -3–5; `unit_price`: 10–883; `discount_pct`: 0–250; `order_amount`: -1593–9576; `delivery_minutes`: 8–1440 | `order_ts`: 2026-09-01 06:30:15–2027-01-01 09:00:00 |
| `payments.csv` | `amount_paid`: -1593–9576 | `payment_ts`: 2026-09-01 06:30:24–2027-01-01 09:01:46 |
| `orders_2026-10-01_increment.csv` | `quantity`: 1–4; `unit_price`: 10–883; `discount_pct`: 0–20; `order_amount`: 11–2052; `delivery_minutes`: 8–34 | `order_ts`: 2026-09-01 09:38:13–2026-10-01 22:33:00 |

Distinct categorical values observed:

| Column | Distinct source values |
| :--- | :--- |
| `orders.order_status` | `DELIVERED`, `RETURNED`, `CANCELLED`, `delivered`, `Pending??` |
| `orders.payment_mode` / `payments.payment_mode` | `UPI`, `Card`, `Cash on Delivery`, `Wallet`, `Cashh`, `upi`, `CARD`, `UPI `, `cod` |
| `payments.payment_status` | `SUCCESS`, `FAILED`, `REFUNDED` |
| `customers.loyalty_tier` | `SILVER`, `Silver`, `Platinum`, `Bronze`, `Gold`, `BRONZE`, `bronze` |
| `stores.zone` | `North`, `South`, `East`, `West`, null |
| `products.category` | `Fruits & Vegetables`, `Dairy & Bakery`, `Staples`, `Snacks & Beverages`, `Personal Care`, `Household`, null |

---

### 2. Data Issues Log (Across All Files)

The complete generated row-level log (493 issue records, with no customer name, email, or phone columns) is [data_issues_log.csv](./data_issues_log.csv).

| File | Row Key(s) | Column | Problem Found | Quality Dimension | Proposed Rule | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `customers.csv` | `C00103`, `C00252`, `C00036`, `C00226`, `C00313` | `customer_id` | Duplicate customer records | Uniqueness | Deduplicate on `customer_id` keeping the first/latest valid record | Critical |
| `customers.csv` | `C00337`, `C00045`, `C00327`, `C00193` | `email` | Missing (`NULL`) email address | Completeness | Flag missing email; mask as `invalid_email` so order joins don't break | Warning |
| `customers.csv` | `C00307`, `C00363`, `C00392`, `C00062`, `C00067` | `email` | Malformed email (missing `@`) | Validity | Regex check for `@`; mask invalid format as `invalid_email` | Warning |
| `customers.csv` | `C00151`, `C00240`, `C00095`, `C00357`, `C00130`, `C00290`, `C00339` | `phone` | Fewer than 10 digits (`min=9804341`) | Validity | Strip non-digits and check `len == 10`; mask invalid as `invalid_phone` | Warning |
| `customers.csv` | `C00400`, `C00398`, `C00038` | `signup_date` | Future signup date (`> 2026-10-01`) | Timeliness | Flag `signup_date > current_date` | Warning |
| `customers.csv` | Multiple rows | `loyalty_tier` | Inconsistent casing (`SILVER`, `Silver`, `bronze`) | Consistency | Standardise with `.str.strip().str.title()` | Warning |
| `products.csv` | `P0030`, `P0140` | `product_id` | Duplicate product keys | Uniqueness | Deduplicate on `product_id` | Critical |
| `products.csv` | `P0051`, `P0060`, `P0068` | `category` | Missing (`NULL`) product category | Completeness | Quarantine or impute `'Uncategorized'` (logged in `dq_results`) | Critical |
| `products.csv` | `P0109` | `mrp` | Negative MRP (`-203.0`) | Accuracy | `mrp > 0` and `cost_price > 0` | Critical |
| `products.csv` | `P0019`, `P0073`, `P0074`, `P0109`, `P0117` | `cost_price`, `mrp` | `cost_price > mrp` (negative product margin) | Accuracy | Flag `cost_price <= mrp` | Warning |
| `stores.csv` | `S07` | `zone` | Missing (`NULL`) zone for BTM Layout store | Completeness | Impute `'South'` (BTM Layout is in South Bengaluru) and log warning | Warning |
| `stores.csv` | `S12` | `capacity_orders_per_day` | Capacity is `0` for active store Rajajinagar | Accuracy | Flag `capacity_orders_per_day <= 0` | Warning |
| `orders.csv` | 12 keys (`ORD001462`, `ORD001326`, etc.) | `order_id` | Duplicate `order_id` rows | Uniqueness | Deduplicate first occurrence; quarantine duplicate rows | Critical |
| `orders.csv` | 18 keys (`ORD002255`, `ORD000701`, etc.) | `customer_id` | `customer_id` is `NULL` | Completeness | Reject/quarantine orders with `NULL` `customer_id` | Critical |
| `orders.csv` | 2 rows (`ORD001121`, `ORD000534`) | `order_ts` | `order_ts` is `NULL` | Completeness | Reject/quarantine orders with `NULL` or unparseable `order_ts` | Critical |
| `orders.csv` | 8 keys (`ORD001554`, `ORD001800`, etc.) | `delivery_minutes` | `DELIVERED` order has `NULL` `delivery_minutes` | Completeness | Reject/quarantine `DELIVERED` orders missing `delivery_minutes` | Critical |
| `orders.csv` | `ORD000986`, `ORD001058`, etc. (9 rows) | `product_id` | Orphan `product_id` not in `products.csv` | Consistency | Reject/quarantine orders where `product_id` not in master | Critical |
| `orders.csv` | `ORD001799`, `ORD001046`, etc. (5 rows) | `store_id` | Orphan `store_id` not in `stores.csv` | Consistency | Reject/quarantine orders where `store_id` not in master | Critical |
| `orders.csv` | Multiple rows | `order_status` | Invalid status `'Pending??'` and lowercase `'delivered'` | Validity | Trim/uppercase `'delivered'` -> `'DELIVERED'`; quarantine `'Pending??'` | Critical |
| `orders.csv` | Multiple rows | `payment_mode` | Typos `'Cashh'`, `'upi'`, `'UPI '`, `'cod'`, `'CARD'` | Validity | Map `'upi'`/`'UPI '`->`'UPI'`, `'CARD'`->`'Card'`, `'cod'`->`'Cash on Delivery'`; quarantine `'Cashh'` | Critical |
| `orders.csv` | 10 keys (`ORD000469`, `ORD001296`, etc.) | `quantity` | `quantity <= 0` (down to `-3`) | Accuracy | Reject/quarantine `quantity <= 0` | Critical |
| `orders.csv` | `ORD002103`, `ORD001468`, `ORD001237` | `discount_pct` | `discount_pct > 100` (up to `250%`) | Validity | Reject/quarantine `discount_pct < 0` or `> 100` | Critical |
| `orders.csv` | 18 keys (`ORD002103`, `ORD002305`, etc.) | `order_amount` | Does not match `round(qty * price * (1 - disc/100), 2)` | Accuracy | Reject/quarantine where `abs(order_amount - expected) > 0.05` | Critical |
| `orders.csv` | 6 keys (`ORD001037`, `ORD002101`, etc.) | `delivery_minutes` | `DELIVERED` order has `delivery_minutes >= 120` (up to `1440`) | Accuracy | Reject/quarantine `delivery_minutes <= 0` or `>= 120` | Critical |
| `orders.csv` | 11 keys (`ORD002119`, `ORD000403`, etc.) | `order_ts` | Future-dated order timestamp (`> 2026-10-01`) | Timeliness | Reject/quarantine future `order_ts` | Critical |
| `payments.csv` | `PAY0001651`, `PAY0000755`, `PAY0000509`, `PAY0002000` | `payment_id` | Duplicate `payment_id` | Uniqueness | Deduplicate first occurrence; quarantine duplicates | Critical |
| `payments.csv` | `PAY0002217` to `PAY0002224` (8 rows) | `order_id` | Orphan `order_id` not found in `orders.csv` | Consistency | Flag in `payment_reconciliation` as `ORPHAN_PAYMENT` | Critical |
| `payments.csv` | 10 keys (`PAY0001568`, `PAY0002175`, etc.) | `amount_paid` | `amount_paid <= 0` | Accuracy | Quarantine/flag invalid negative/zero payment amounts | Critical |
| `payments.csv` | 14 keys (`PAY0001568`, `PAY0000354`, etc.) | `amount_paid` | `SUCCESS` payment does not equal `order_amount` | Consistency | Flag in `payment_reconciliation` as `AMOUNT_MISMATCH` | Critical |

---

### 3. Three Clarifying Questions for the Client (Kavya & Sameer)
1. **Master Dimension Defects (`stores.csv` & `products.csv`):** Store `S07` (BTM Layout) has a null `zone`, and products `P0051`, `P0060`, `P0068` have a null `category` (plus `P0109` has a negative MRP of `-203.0`). If a valid order references one of these products or stores, should we **quarantine the order**, or should we **impute/repair the master dimension** (e.g., mapping BTM Layout to `'South'` zone and missing categories to `'Uncategorized'`) so valid store revenue isn't lost?
2. **Payment Mode Typos (`'Cashh'` vs `'cod'`):** We standardise obvious casing/abbreviation variants (`'upi'`/`'UPI '` → `'UPI'`, `'cod'` → `'Cash on Delivery'`, `'CARD'` → `'Card'`). Should `'Cashh'` also be mapped to `'Cash on Delivery'` as a known POS typo, or quarantined as an invalid payment mode?
3. **Formula Mismatches (`order_amount`):** For the 18 orders where `order_amount` disagrees with `quantity * unit_price * (1 - discount_pct/100)` (despite valid positive quantity and price), is the OMS formula calculation buggy (meaning we should recompute `order_amount`), or should those 18 orders be strictly quarantined until Finance reviews them against the payment gateway?

## Part C: Scheduling

- **Cron:** `0 9 * * 1-6` (09:00, Monday through Saturday).
- **Timezone:** `Asia/Kolkata`; `start_date=pendulum.datetime(2026, 9, 1, 9, 0, tz="Asia/Kolkata")`.
- **UTC display:** 09:00 IST is 03:30 UTC. If the Airflow UI is configured to display UTC, the scheduled logical time is 03:30.
- **If `start_date` were UTC:** the cron would run at 09:00 UTC, which is 14:30 IST, missing the morning SLA.
- **`catchup=False`:** avoids creating a run for every historical schedule interval when the scheduler starts after downtime. A scheduler still needs to be online at the scheduled time; if the laptop/Docker is off, the 09:00 report is not produced. When Airflow is restarted, it may schedule the latest completed interval once, so the run can be late; this is not an SLA recovery plan.
- **Observed locally:** both DAGs are unpaused and the UI shows `0 9 * * 1-6`. Airflow started the previous logical interval (`2026-10-21 09:00 IST`) late on 22 Oct; it completed successfully after recovery. The next on-time 09:00 IST firing has not yet been observed.

## Part D: Data Quality Dimensions

Both pipelines write a per-run quality scorecard to `dq_results`, with the required columns. Critical failures are rejected from the finance facts; warnings are standardized or retained with a warning status. For the latest successful manual runs, ETL recorded **31 checks** and ELT **15 checks**. Summed `rows_failed` values are check failures, not unique source rows, because one row can fail multiple checks.

| Dimension | ETL example | ELT example | Severity |
| :--- | :--- | :--- | :--- |
| Completeness | Required customer ID and delivery duration on delivered orders | `orders_required_customer`; missing product category | Critical |
| Uniqueness | Duplicate order, customer, product, and payment keys | `key_rank` / duplicate-key checks in staging | Critical |
| Validity | Allowed order status/payment mode, discount bounds, parseable timestamp; email/phone format | SQL allowed-value and customer-contact checks | Critical for order values; Warning for contact format |
| Accuracy | Quantity and amount formula; positive product prices; delivery duration bounds | SQL amount, quantity, price, and delivery checks | Critical |
| Consistency | Customer/product/store foreign keys and payment reconciliation | SQL order-master foreign keys and payment-order existence | Critical |
| Timeliness | No future timestamp; latest timestamp within one day of run time | SQL future-date and runtime freshness checks | Critical for future timestamps; Warning for stale data |

The supplied snapshot is historical: the most recent accepted order timestamp is 1 Oct 2026, so the runtime one-day freshness check correctly records a warning when run on 22 Oct 2026.

## Business Questions: SQL and Results

The following are the reporting queries and observed results after the increment was applied. The tables shown for each question are identical in ETL and ELT unless a difference is called out.

### 1. Daily net revenue by zone and store

```sql
-- ETL
SELECT order_date, zone, store_id, store_name,
       SUM(delivered_orders) AS delivered_orders,
       ROUND(SUM(daily_net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
WHERE order_date = DATE '2026-10-01'
GROUP BY 1, 2, 3, 4
ORDER BY store_id;

-- ELT: replace daily_net_revenue with net_revenue and use mart_daily_store_kpis.
SELECT order_date, zone, store_id, store_name,
       SUM(delivered_orders) AS delivered_orders,
       ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE order_date = DATE '2026-10-01'
GROUP BY 1, 2, 3, 4
ORDER BY store_id;
```

| Store | Zone | Delivered orders | Net revenue |
| :--- | :--- | ---: | ---: |
| S01 — Hebbal | North | 11 | 6,780.25 |
| S02 — Yelahanka | North | 9 | 4,070.80 |
| S03 — RT Nagar | North | 6 | 3,695.75 |
| S04 — Jayanagar | South | 5 | 2,168.00 |
| S05 — JP Nagar | South | 11 | 7,491.20 |
| S06 — Banashankari | South | 6 | 4,893.55 |
| S07 — BTM Layout | South | 5 | 2,656.40 |
| S08 — Whitefield | East | 15 | 6,569.90 |
| S09 — Indiranagar | East | 8 | 2,610.05 |
| S10 — Marathahalli | East | 11 | 4,522.90 |
| S11 — HSR Layout | East | 17 | 8,238.60 |
| S12 — Rajajinagar | West | 6 | 5,554.25 |
| S13 — Malleshwaram | West | 9 | 4,498.65 |
| S14 — Vijayanagar | West | 14 | 6,145.50 |
| S15 — Basaveshwaranagar | West | 4 | 2,664.40 |

### 2. Revenue and gross margin by category

```sql
-- ETL: SELECT category, delivered_orders, total_revenue, total_gross_margin
--      FROM etl_category_margin ORDER BY category;
-- ELT: SELECT category, delivered_orders, revenue, gross_margin
--      FROM mart_category_margin ORDER BY category;
```

| Category | Delivered orders | Revenue | Gross margin |
| :--- | ---: | ---: | ---: |
| Dairy & Bakery | 405 | 179,568.95 | 47,633.00 |
| Fruits & Vegetables | 374 | 93,604.20 | 22,205.38 |
| Household | 362 | 211,727.05 | 54,352.51 |
| Personal Care | 323 | 210,889.10 | 47,054.58 |
| Snacks & Beverages | 340 | 174,700.60 | 41,370.78 |
| Staples | 302 | 268,583.30 | 67,143.14 |

### 3. Average delivery time and 15-minute SLA by store

```sql
-- ETL: SELECT * FROM etl_delivery_sla ORDER BY store_id;
-- ELT: SELECT * FROM mart_delivery_sla ORDER BY store_id;
```

| Store | Avg. minutes | Delivered orders | Within 15 min |
| :--- | ---: | ---: | ---: |
| S01 Hebbal | 20.62 | 141 | 31.21% |
| S02 Yelahanka | 20.99 | 139 | 30.94% |
| S03 RT Nagar | 20.74 | 138 | 28.99% |
| S04 Jayanagar | 21.59 | 130 | 26.92% |
| S05 JP Nagar | 21.24 | 134 | 29.10% |
| S06 Banashankari | 22.45 | 147 | 21.77% |
| S07 BTM Layout | 21.08 | 144 | 33.33% |
| S08 Whitefield | 20.60 | 157 | 35.67% |
| S09 Indiranagar | 21.16 | 129 | 27.13% |
| S10 Marathahalli | 21.90 | 143 | 27.27% |
| S11 HSR Layout | 21.01 | 145 | 27.59% |
| S12 Rajajinagar | 21.99 | 113 | 23.89% |
| S13 Malleshwaram | 21.73 | 138 | 27.54% |
| S14 Vijayanagar | 21.57 | 163 | 31.29% |
| S15 Basaveshwaranagar | 22.43 | 145 | 20.69% |

### 4. Revenue by payment mode and reconciliation discrepancy

```sql
-- ETL: SELECT * FROM etl_payment_mode_revenue ORDER BY payment_mode;
-- ELT: SELECT * FROM mart_payment_mode_revenue ORDER BY payment_mode;
-- Both reconciliation tables:
SELECT mismatch_reason, COUNT(*) AS rows,
       ROUND(SUM(ABS(discrepancy_amount)), 2) AS absolute_discrepancy
FROM etl_payment_reconciliation
GROUP BY mismatch_reason ORDER BY mismatch_reason;
```

| Payment mode | Delivered revenue |
| :--- | ---: |
| Card | 236,404.25 |
| Cash on Delivery | 170,735.40 |
| UPI | 602,955.75 |
| Wallet | 128,977.80 |

The two reconciliation tables each contain **433** mismatch rows and **244,664.00** total absolute discrepancy. The categories agree: 163 delivered orders missing payment (88,941.55), 42 delivered orders with a non-success payment (0.00 amount difference), 207 orphan payments (155,062.45), 9 returned orders not refunded (0.00 amount difference), and 12 successful amount mismatches (660.00). ETL calls the orphan category `ORPHAN_PAYMENT_NO_VALID_ORDER`; ELT calls it `ORPHAN_PAYMENT`.

### 5. Top 10 customers by delivered spend

```sql
-- ETL: SELECT customer_id, loyalty_tier, total_spend, spend_rank
--      FROM etl_top_customers ORDER BY spend_rank;
-- ELT: SELECT customer_id, loyalty_tier, total_spend, spend_rank
--      FROM mart_top_customers ORDER BY spend_rank;
```

| Rank | Customer ID | Loyalty tier | Spend |
| ---: | :--- | :--- | ---: |
| 1 | C00136 | Bronze | 8,738.10 |
| 2 | C00236 | Platinum | 7,831.80 |
| 3 | C00115 | Bronze | 7,337.70 |
| 4 | C00310 | Bronze | 7,258.95 |
| 5 | C00265 | Gold | 7,151.30 |
| 6 | C00338 | Silver | 6,848.55 |
| 7 | C00283 | Bronze | 6,766.40 |
| 8 | C00357 | Bronze | 6,637.90 |
| 9 | C00209 | Platinum | 6,519.40 |
| 10 | C00072 | Silver | 6,433.95 |

No customer name, email, or phone appears in either result.

### 6. Rejections by file and reason

```sql
-- ETL
SELECT source_file, failure_reason, COUNT(*) AS rejected_rows
FROM etl_rejections
GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2;

-- ELT
SELECT source_file, failure_reason, COUNT(*) AS rejected_rows
FROM mart_rejections
GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2;
```

| File | ETL rejected rows | ELT rejected rows | Representative reasons |
| :--- | ---: | ---: | :--- |
| `customers.csv` | 5 | 5 | Duplicate customer key |
| `orders.csv` | 220 | 219 | Orphan product/store, null customer, duplicate order, amount formula mismatch, invalid status/mode, invalid/future timestamp, delivery/quantity/discount rules |
| `orders_2026-10-01_increment.csv` | 14 | 14 | Orphan product reference |
| `payments.csv` | 16 | 16 | Non-positive amount, duplicate payment, invalid mode |
| `products.csv` | 10 | 10 | Cost above MRP, missing category, duplicate product, non-positive price |
| `stores.csv` | 2 | 0 | ETL quarantines the missing zone and zero capacity as warnings; ELT retains those rows with `dq_status='WARN'` |

The full grouped SQL above returns each individual reason (including combined multi-rule reasons); the totals reconcile to the latest audit: **267 ETL** and **264 ELT** quarantined rows. ELT also retains every source row in `raw_*`.

## Part E: Data Governance

1. **Ownership.** Operations owns order and store definitions; Finance owns payment reconciliation and revenue rules; CRM owns customers; Merchandising owns products. A quality-rule change (such as the delivery ceiling) requires joint approval from Operations and the business owner affected, with Finance approval if revenue eligibility changes. The data/engineering owner implements and versions the rule.

2. **Data dictionary.**

| Table / columns | Type and meaning | Source / allowed values | Personal data? |
| :--- | :--- | :--- | :--- |
| `etl_fact_orders` / `stg_orders`: `order_id`, `customer_id`, `store_id`, `product_id` | String keys linking the order and dimensions | `orders.csv` and increment; IDs must exist in their respective masters | `customer_id` is pseudonymous and linkable |
| `etl_fact_orders` / `stg_orders`: `order_ts`, `order_date` | Timestamp and derived calendar date | Order management system | No |
| `etl_fact_orders` / `stg_orders`: `quantity`, `unit_price`, `discount_pct`, `order_amount`, `net_revenue`, `gross_margin` | Integer/decimal; quantity, listed prices, discounted order total, delivered revenue, revenue minus quantity × cost | `orders.csv` and `products.csv`; quantity > 0, discount 0–100, money in source currency | No |
| `etl_fact_orders` / `stg_orders`: `order_status`, `payment_mode` | Standardized string categories | `DELIVERED`, `CANCELLED`, `RETURNED`; `UPI`, `Card`, `Cash on Delivery`, `Wallet` | No |
| `etl_fact_orders` / `stg_orders`: `delivery_minutes`, `within_15_min` | Decimal minutes and Boolean SLA flag | `orders.csv`; delivered orders need duration; valid duration is 0–120 | No |
| `etl_dim_customer` / `stg_customers`: `customer_id`, `loyalty_tier`, `is_active`, `signup_date`, `city` | String key/category, Boolean, date, city string | CRM; four title-cased loyalty tiers | Customer-linked |
| Customer dimensions: `full_name`, `email`, `phone` | String display/contact values, masked before staging/reporting | CRM; invalid contacts use explicit invalid markers | Yes; masked |
| Customer dimensions: `area`, `pincode` | String/integer location fields; area is `Restricted`, pincode is null in curated dimensions | CRM; raw values remain only in restricted raw source | Yes/quasi-identifying; suppressed in curated dimensions |
| `etl_dim_product` / `stg_products`: `product_id`, `product_name`, `category`, `sub_category`, `brand`, `unit`, `is_perishable`, `launch_date`, `mrp`, `cost_price` | String labels, Boolean, date, decimal prices | Product catalogue; six observed categories, positive prices required | No |
| `etl_dim_store` / `stg_stores`: `store_id`, `store_name`, `zone`, `area`, `pincode`, `open_date`, `capacity_orders_per_day` | String key/location, date, integer capacity | Store master; zones North/South/East/West | Business location, not customer PII |
| Store dimension: `manager_name` | String name masked to initial plus `***` | Store master | Yes; masked |
| `etl_*_revenue`, `etl_*_sla`, `mart_*` | Aggregated decimal/count/rank outputs | Derived from accepted fact/staging rows | No direct email/phone/name |

3. **Lineage.** The ELT cell `2026-09-01 / S12 / net_revenue=300.00` comes from delivered order `ORD000014` in `orders.csv`, data row 1,181 (`_source_row=1181` in `raw_orders`); the record has quantity 2, unit price 150, zero discount, and amount 300. ETL can trace the accepted fact by `order_id`, but does not preserve the source row number. ELT is easier to audit because it preserves `_source_file`, `_source_row`, `_run_id`, and the untouched raw row.

4. **Personal data and legal basis.** Direct personal fields are customer `full_name`, `email`, and `phone`; customer-linked `city`, `area`, and `pincode` are location data; store `manager_name` identifies an employee; `customer_id` is pseudonymous but linkable. Curated customer dimensions mask name/email/phone, suppress area/pincode, and retain only city-level location; the employee name is masked. Raw PII remains unchanged only for restricted audit access. The Digital Personal Data Protection Act, 2023 (India) is relevant; the organization should provide notice and a lawful purpose, limit collection/use, secure data, support applicable data-principal rights, honor retention/erasure obligations, and follow breach procedures. Legal counsel should validate the production policy.

5. **Access control.** Raw tables and quarantine files should be restricted to a small data-steward/security group; raw customer PII should not be visible to ordinary analysts. Staging access should be limited to data engineering and approved analysts; marts can be available to operations and finance. This local DuckDB prototype does not enforce database roles, so these are deployment requirements, not a claim that local ACLs exist.

6. **Retention.** Proposed policy: retain raw rows only for the approved audit window (for example, 13 months, subject to finance/legal requirements); quarantine records for 90 days after resolution; masked customer dimensions while the account is active plus the required financial retention period. At expiry, securely delete or irreversibly aggregate/anonymize; legal holds override routine expiry.

7. **Secrets.** The local pipeline needs no API credentials. A future Slack webhook belongs in an Airflow Connection or `Variable`, not the DAG, source CSV, logs, or Git history. This avoids accidental disclosure and permits rotation/access control.

8. **Rejected-order audit.** `ORD001459` is present as received in ELT raw row 815 of `orders.csv`: quantity `-3`, unit price `531.0`, discount `0`, amount `-1593.0`, status `DELIVERED`, product `P0060`. ETL `etl_rejections` records `NON_POSITIVE_QUANTITY;ORPHAN_PRODUCT_ID`; the row is absent from `etl_fact_orders`. ELT retains the raw values, records the same logical failures in staging/rejections, and excludes it from the current passing rows used by marts.

9. **Change history.** `ORD000017` was `DELIVERED` in `orders.csv` on 1 September (amount 16.00) and `RETURNED` in the 1 October increment. Both current-state pipelines now show `RETURNED`, so it contributes zero to current September revenue; Finance should use this corrected current-state answer. ELT retains both raw versions for audit. To report both “as originally booked” and “current restated,” production should add effective-time/event history (or an SCD Type 2 order-status history) and publish separate as-of and current marts.

## Part F: Observability

### Experiment results

“Not run” means no destructive source modification was made and no observed result is claimed. The source files remain unchanged.

| # | Expected | Observed | Where observed | Follow-up |
| ---: | :--- | :--- | :--- | :--- |
| 1 Happy path | Five ETL extracts can run in parallel; all tasks finish; audit and quality rows appear | ETL and ELT manual runs succeeded. Latest: extracted 5,372 each; loaded 2,886 ETL / 2,329 ELT; quarantined 267 / 264. | Airflow task runs and DuckDB `pipeline_runs`, `dq_results` | Add alerting on failed status and stale freshness warning |
| 2 Re-run | ETL fact/marts unchanged; ELT raw grows by exactly source rows; stage/marts unchanged | ETL facts remain 2,329. ELT raw is append-only; a per-run source guard prevents retry duplication. One earlier run appended orders twice (7,944 audit extraction); it was retained in raw, and a subsequent guarded run reported 5,372. | Warehouse counts and `pipeline_runs` | Keep append-once guard; monitor raw row counts per run |
| 3 Increment | 150 inserts, 10 status updates, no duplicate current order | Observed 150 increment rows `DELIVERED`, 10 `RETURNED`; 2,329 current passing orders in each pipeline; 10 corrected statuses in staging | `stg_orders`, `etl_fact_orders`, increment CSV, mart totals | Preserve event history to report both original and corrected status |
| 4 Missing source | `FileNotFoundError`, two retries, then failed task and downstream tasks | Not run against a renamed source file. Code checks existence before reading; retry states/timing remain unobserved. | No experiment log | Run in an isolated test copy; alert after final retry |
| 5 Five injected bad rows | Null customer, negative quantity, unknown store, invalid status, and future timestamp each get a reason and DQ result | Not run; no injected rows or observed result claimed. | No experiment log | Execute on a disposable input copy and compare quarantine plus `dq_results` |
| 6 Schema rename | Missing `order_amount` causes an explicit transform/SQL error, not a silent bad load | Not run; failure task and exact exception remain unobserved. | No experiment log | Add schema-contract checks before load and retain the failing-run log |
| 7 50-order volume | A warning for a >90% drop from baseline | Not injected. ETL warns below 500 orders; ELT warns below 242 accepted orders (10% of the 2,412-row base). | Latest `dq_results` contains volume checks; 50-row response not observed | Set a rolling baseline and alert on warning |
| 8 Price distribution ×100 | Affected store revenue moves sharply; current checks may not detect distribution drift | Not run; no measured delta. There is no implemented store-level price-distribution anomaly check. | No experiment log | Add per-store price/revenue distribution baselines and drift thresholds |
| 9 30-day stale input | Freshness warning | The injected 30-day file was not run. The baseline latest successful run does show `orders_freshness_within_1_day` / `orders_freshness_1d` as `WARN` because the supplied snapshot ends on 1 Oct while the run date is 22 Oct. | `dq_results` and Airflow run log | Alert on stale warning; test with an isolated 30-day input |
| 10 Recovery | Clearing failed task plus downstream reruns only the necessary work and records final success | The scheduled ELT run initially hit DuckDB writer-lock errors. After clearing the failed customer/payment loads and their downstream tasks, both loads, staging, marts, and the audit task succeeded. The run-level guard prevented duplicate raw appends; `pipeline_runs` reports 5,372 extracted, 2,329 loaded, 264 quarantined, `SUCCESS`. The original on-time schedule was missed; this was a late recovery, not proof of the SLA. | Airflow task-state view and `pipeline_runs` for `scheduled__2026-10-21T03:30:00+00:00` | Keep serialized database access; alert on lock contention and late schedule recovery |

### Observability pillars

| Pillar | Current coverage |
| :--- | :--- |
| Freshness | Runtime one-day timeliness check in both pipelines; stale rows create a warning in `dq_results`. |
| Volume | Minimum-volume warning in both pipelines; should be replaced by a rolling baseline for production. |
| Schema | Missing columns fail in the Python/SQL transforms, but there is no separate schema-contract table/check yet. |
| Distribution | No active drift detector; weakest pillar. Add per-store/product price and order-mix baselines. |
| Lineage | Best-covered pillar in ELT: raw source, row position, run ID and append-only source history. ETL preserves row keys but not row positions. |

## Part G: Recommendation to Sameer (CTO)

**Recommendation: adopt ELT with a strictly governed finance mart as the reporting interface.** The requirement is not to expose every raw record to Finance; it is to keep the source history for audit while ensuring the CFO reads only validated current rows. The implementation demonstrates that separation: raw tables retain every source value, staging marks quality status and rejection reasons, and marts select only current passing records. The loaded accepted count and delivered revenue match ETL (2,329 and 1,139,073.20); September revenue also matches (1,066,513.00). The increment’s ten returned-order corrections are visible in current marts while both prior and new source records remain auditable in raw.

ELT is easier to adapt as Finance changes a rule because staging/mart logic is versioned SQL and raw records can be replayed. It also provides better lineage and preserves the ability to answer future questions. ETL remains valuable as a comparison and offers a straightforward “validate before fact load” guarantee; it should remain a tested control until the finance mart’s gate and reconciliation are accepted.

The current stack is open source and local, so it has no warehouse license cost; compute, storage, backup, and operations still have costs. At 1,000× input volume (about 5.4 million source rows for this snapshot), SQL transformations remain a good fit, but a single local DuckDB file is not a production concurrency/availability plan. Move raw data to partitioned Parquet/object storage and use a managed or shared analytical engine if concurrency, SLA, or volume exceeds the local deployment. The scheduled-run retry exposed concurrent writers against DuckDB; the final design serializes connections with a local file lock and makes raw appends idempotent per run. Production should use a durable shared lock/transaction-capable warehouse rather than rely on one host’s filesystem lock.

**Top three risks:** (1) unmasked raw PII requires strict access, encryption, retention, and audit controls; (2) DuckDB and a laptop/Docker scheduler are single-host and cannot guarantee the SLA through power/network outages; (3) the current acceptance runs did not execute all destructive observability experiments, and distribution-drift detection remains absent. Complete those tests and add alerts before production sign-off.how