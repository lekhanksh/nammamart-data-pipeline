import os
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from include.nammamart_helpers import (
    DATA_DIR, mask_email, mask_phone, mask_name, standardise_payment_mode,
    init_db, save_dq_results, warehouse_connection, write_quarantine
)

IST = pendulum.timezone("Asia/Kolkata")
CUTOFF_DATE = "2026-10-01 23:59:59"
ALLOWED_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAY_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}

default_args = {
    "owner": "lekhanksh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="nammamart_etl_lekhanksh",
    default_args=default_args,
    description="NammaMart ETL Pipeline: Extract -> Validate & Quarantine -> Transform -> Load DuckDB Star Schema",
    schedule="0 9 * * 1-6",  # 9:00 AM IST, Monday to Saturday
    start_date=pendulum.datetime(2026, 9, 1, 9, 0, tz=IST),
    catchup=False,
    tags=["nammamart", "etl", "fde"],
)
def nammamart_etl_pipeline():

    # 1. EXTRACT: Five parallel tasks (one per master/daily file)
    @task
    def extract_customers():
        path = f"{DATA_DIR}/customers.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        df = pd.read_csv(path)
        return {"file": "customers", "path": path, "rows": len(df)}

    @task
    def extract_products():
        path = f"{DATA_DIR}/products.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        df = pd.read_csv(path)
        return {"file": "products", "path": path, "rows": len(df)}

    @task
    def extract_stores():
        path = f"{DATA_DIR}/stores.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        df = pd.read_csv(path)
        return {"file": "stores", "path": path, "rows": len(df)}

    @task
    def extract_orders():
        path = f"{DATA_DIR}/orders.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        df = pd.read_csv(path)
        return {"file": "orders", "path": path, "rows": len(df)}

    @task
    def extract_payments():
        path = f"{DATA_DIR}/payments.csv"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing source file: {path}")
        df = pd.read_csv(path)
        return {"file": "payments", "path": path, "rows": len(df)}

    # 2, 3, 4 & 5. VALIDATE, QUARANTINE, TRANSFORM & LOAD STAR SCHEMA
    @task
    def validate_transform_load(c_info, p_info, s_info, o_info, pay_info, **context):
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        start_ts = datetime.utcnow().isoformat()

        df_c = pd.read_csv(c_info["path"])
        df_p = pd.read_csv(p_info["path"])
        df_s = pd.read_csv(s_info["path"])
        df_o = pd.read_csv(o_info["path"])
        df_pay = pd.read_csv(pay_info["path"])

        # Incremental feed support: if orders_2026-10-01_increment.csv exists in params or folder
        load_increment = context["params"].get("load_increment", True)
        inc_path = f"{DATA_DIR}/orders_2026-10-01_increment.csv"
        inc_rows = 0
        if load_increment and os.path.exists(inc_path):
            df_inc = pd.read_csv(inc_path)
            inc_rows = len(df_inc)

        dq_checks = []
        total_quarantined = 0
        rejection_frames = []

        def add_dq(name, dim, sev, total, failed):
            dq_checks.append({
                "check_name": name,
                "dimension": dim,
                "severity": sev,
                "rows_checked": int(total),
                "rows_failed": int(failed),
                "status": "PASS" if int(failed) == 0 else ("WARN" if sev == "Warning" else "FAIL"),
            })

        def collect_rejections(df, source_file, key_column):
            if df.empty:
                return
            rejected = df[[key_column, "failure_reason"]].copy()
            rejected = rejected.rename(columns={key_column: "row_key"})
            rejected.insert(0, "source_file", source_file)
            rejection_frames.append(rejected[["source_file", "row_key", "failure_reason"]])

        # --- A. STORES VALIDATION & CLEANING ---
        s_null_zone = df_s["zone"].isna()
        s_zero_cap = df_s["capacity_orders_per_day"] <= 0
        add_dq("stores_zone_not_null", "Completeness", "Warning", len(df_s), s_null_zone.sum())
        add_dq("stores_capacity_positive", "Accuracy", "Warning", len(df_s), s_zero_cap.sum())

        # Quarantine record for audit visibility while repairing BTM Layout -> South for joins
        s_bad = df_s[s_null_zone | s_zero_cap].copy()
        if not s_bad.empty:
            total_quarantined += len(s_bad)
            s_bad["failure_reason"] = ""
            s_bad.loc[s_null_zone, "failure_reason"] += "NULL_ZONE;"
            s_bad.loc[s_zero_cap, "failure_reason"] += "ZERO_CAPACITY;"
            write_quarantine(s_bad, "stores", run_id)
            collect_rejections(s_bad, "stores.csv", "store_id")

        df_s_clean = df_s.copy()
        df_s_clean["store_id"] = df_s_clean["store_id"].astype(str).str.strip()
        df_s_clean["zone"] = df_s_clean["zone"].fillna("South").astype(str).str.strip().str.title()
        df_s_clean["manager_name"] = df_s_clean["manager_name"].apply(mask_name)

        # --- B. CUSTOMERS VALIDATION, DEDUPLICATION & PII MASKING ---
        c_dup = df_c.duplicated(subset=["customer_id"], keep="first")
        c_null_email = df_c["email"].isna()
        c_bad_email = ~df_c["email"].fillna("").astype(str).str.contains("@")
        c_digits = df_c["phone"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.replace(r"\D", "", regex=True)
        c_bad_phone = c_digits.str.len() != 10
        c_future_signup = df_c["signup_date"].astype(str) > "2026-10-01"
        c_bad_casing = ~df_c["loyalty_tier"].astype(str).isin(["Bronze", "Silver", "Gold", "Platinum"])

        add_dq("customers_unique_id", "Uniqueness", "Critical", len(df_c), c_dup.sum())
        add_dq("customers_email_valid", "Validity", "Warning", len(df_c), (c_null_email | c_bad_email).sum())
        add_dq("customers_phone_10_digits", "Validity", "Warning", len(df_c), c_bad_phone.sum())
        add_dq("customers_signup_not_future", "Timeliness", "Warning", len(df_c), c_future_signup.sum())
        add_dq("customers_loyalty_tier_casing", "Consistency", "Warning", len(df_c), c_bad_casing.sum())

        df_c_failed = df_c[c_dup].copy()
        df_c_failed["failure_reason"] = "DUPLICATE_CUSTOMER_ID"
        write_quarantine(df_c_failed, "customers", run_id)
        collect_rejections(df_c_failed, "customers.csv", "customer_id")
        total_quarantined += len(df_c_failed)

        df_c_clean = df_c[~c_dup].copy()
        df_c_clean["customer_id"] = df_c_clean["customer_id"].astype(str).str.strip()
        df_c_clean["full_name"] = df_c_clean["full_name"].apply(mask_name)
        df_c_clean["email"] = df_c_clean["email"].apply(mask_email)
        df_c_clean["phone"] = df_c_clean["phone"].apply(mask_phone)
        df_c_clean["city"] = df_c_clean["city"].fillna("Bengaluru").astype(str).str.strip()
        df_c_clean["area"] = "Restricted"
        df_c_clean["pincode"] = pd.NA
        df_c_clean["loyalty_tier"] = df_c_clean["loyalty_tier"].astype(str).str.strip().str.title()

        # --- C. PRODUCTS VALIDATION & CLEANING ---
        p_dup = df_p.duplicated(subset=["product_id"], keep="first")
        p_null_cat = df_p["category"].isna()
        p_neg_mrp = (df_p["mrp"] <= 0) | (df_p["cost_price"] <= 0)
        p_cost_gt_mrp = df_p["cost_price"] > df_p["mrp"]

        add_dq("products_unique_id", "Uniqueness", "Critical", len(df_p), p_dup.sum())
        add_dq("products_category_not_null", "Completeness", "Critical", len(df_p), p_null_cat.sum())
        add_dq("products_mrp_positive", "Accuracy", "Critical", len(df_p), p_neg_mrp.sum())
        add_dq("products_cost_le_mrp", "Accuracy", "Critical", len(df_p), p_cost_gt_mrp.sum())

        p_crit_mask = p_dup | p_null_cat | p_neg_mrp | p_cost_gt_mrp
        df_p_failed = df_p[p_crit_mask].copy()
        reasons_p = []
        for idx, row in df_p_failed.iterrows():
            r = []
            if p_dup.loc[idx]: r.append("DUPLICATE_PRODUCT_ID")
            if p_null_cat.loc[idx]: r.append("NULL_CATEGORY")
            if p_neg_mrp.loc[idx]: r.append("NON_POSITIVE_MRP_OR_COST")
            if p_cost_gt_mrp.loc[idx]: r.append("COST_GREATER_THAN_MRP")
            reasons_p.append(";".join(r))
        df_p_failed["failure_reason"] = reasons_p
        write_quarantine(df_p_failed, "products", run_id)
        collect_rejections(df_p_failed, "products.csv", "product_id")
        total_quarantined += len(df_p_failed)

        df_p_clean = df_p[~p_crit_mask].copy()
        df_p_clean["product_id"] = df_p_clean["product_id"].astype(str).str.strip()
        df_p_clean["category"] = df_p_clean["category"].astype(str).str.strip()

        # --- D. ORDERS VALIDATION (All 6 Dimensions) ---
        def validate_orders_df(df_ord, is_increment=False):
            o_dup = df_ord.duplicated(subset=["order_id"], keep="first")
            o_null_cust = df_ord["customer_id"].isna()
            o_null_ts = df_ord["order_ts"].isna()

            parsed_ts = pd.to_datetime(df_ord["order_ts"], errors="coerce")
            o_bad_ts = parsed_ts.isna()
            o_future_ts = parsed_ts > pd.to_datetime(CUTOFF_DATE)

            status_std = df_ord["order_status"].fillna("").astype(str).str.strip().str.upper()
            o_bad_status = ~status_std.isin(ALLOWED_STATUS)

            pay_std = df_ord["payment_mode"].apply(standardise_payment_mode)
            o_bad_paymode = ~pay_std.isin(ALLOWED_PAY_MODES)

            o_bad_qty = df_ord["quantity"].isna() | (df_ord["quantity"] <= 0)
            o_bad_disc = df_ord["discount_pct"].isna() | (df_ord["discount_pct"] < 0) | (df_ord["discount_pct"] > 100)

            expected_amt = (df_ord["quantity"] * df_ord["unit_price"] * (1.0 - df_ord["discount_pct"] / 100.0)).round(2)
            o_bad_formula = (df_ord["order_amount"] - expected_amt).abs() > 0.05

            is_deliv = status_std == "DELIVERED"
            o_deliv_null_mins = is_deliv & df_ord["delivery_minutes"].isna()
            o_deliv_bad_mins = is_deliv & (~df_ord["delivery_minutes"].isna()) & (
                (df_ord["delivery_minutes"] <= 0) | (df_ord["delivery_minutes"] >= 120)
            )

            o_orphan_cust = (~o_null_cust) & (~df_ord["customer_id"].astype(str).str.strip().isin(set(df_c_clean["customer_id"])))
            o_orphan_prod = ~df_ord["product_id"].astype(str).str.strip().isin(set(df_p_clean["product_id"]))
            o_orphan_store = ~df_ord["store_id"].astype(str).str.strip().isin(set(df_s_clean["store_id"]))

            if not is_increment:
                add_dq("orders_unique_id", "Uniqueness", "Critical", len(df_ord), o_dup.sum())
                add_dq("orders_customer_not_null", "Completeness", "Critical", len(df_ord), o_null_cust.sum())
                add_dq("orders_delivered_has_minutes", "Completeness", "Critical", len(df_ord), o_deliv_null_mins.sum())
                add_dq("orders_valid_status", "Validity", "Critical", len(df_ord), o_bad_status.sum())
                add_dq("orders_valid_payment_mode", "Validity", "Critical", len(df_ord), o_bad_paymode.sum())
                add_dq("orders_valid_discount_pct", "Validity", "Critical", len(df_ord), o_bad_disc.sum())
                add_dq("orders_positive_quantity", "Accuracy", "Critical", len(df_ord), o_bad_qty.sum())
                add_dq("orders_amount_formula_match", "Accuracy", "Critical", len(df_ord), o_bad_formula.sum())
                add_dq("orders_delivery_minutes_realistic", "Accuracy", "Critical", len(df_ord), o_deliv_bad_mins.sum())
                add_dq("orders_fk_customer_exists", "Consistency", "Critical", len(df_ord), o_orphan_cust.sum())
                add_dq("orders_fk_product_exists", "Consistency", "Critical", len(df_ord), o_orphan_prod.sum())
                add_dq("orders_fk_store_exists", "Consistency", "Critical", len(df_ord), o_orphan_store.sum())
                add_dq("orders_ts_not_future", "Timeliness", "Critical", len(df_ord), (o_bad_ts | o_future_ts).sum())

                # Volume & Freshness checks (Part F Observability)
                vol_fail = 1 if len(df_ord) < 500 else 0
                add_dq("orders_volume_min_threshold", "Completeness", "Warning", len(df_ord), vol_fail)
                max_ts = parsed_ts[~o_future_ts].max()
                now = pd.Timestamp.now(tz="UTC").tz_localize(None)
                stale_fail = 1 if (pd.notna(max_ts) and now - max_ts > pd.Timedelta(days=1)) else 0
                add_dq("orders_freshness_within_1_day", "Timeliness", "Warning", len(df_ord), stale_fail)

            fail_mask = (
                o_dup | o_null_cust | o_bad_ts | o_future_ts | o_bad_status |
                o_bad_paymode | o_bad_qty | o_bad_disc | o_bad_formula |
                o_deliv_null_mins | o_deliv_bad_mins | o_orphan_cust | o_orphan_prod | o_orphan_store
            )

            df_bad = df_ord[fail_mask].copy()
            reasons = []
            for idx in df_bad.index:
                r = []
                if o_dup.loc[idx]: r.append("DUPLICATE_ORDER_ID")
                if o_null_cust.loc[idx]: r.append("NULL_CUSTOMER_ID")
                if o_bad_ts.loc[idx]: r.append("NULL_OR_INVALID_ORDER_TS")
                elif o_future_ts.loc[idx]: r.append("FUTURE_ORDER_TS")
                if o_bad_status.loc[idx]: r.append("INVALID_ORDER_STATUS")
                if o_bad_paymode.loc[idx]: r.append("INVALID_PAYMENT_MODE")
                if o_bad_qty.loc[idx]: r.append("NON_POSITIVE_QUANTITY")
                if o_bad_disc.loc[idx]: r.append("DISCOUNT_OUT_OF_RANGE")
                if o_bad_formula.loc[idx]: r.append("ORDER_AMOUNT_FORMULA_MISMATCH")
                if o_deliv_null_mins.loc[idx]: r.append("DELIVERED_MISSING_MINUTES")
                if o_deliv_bad_mins.loc[idx]: r.append("DELIVERY_MINUTES_OUT_OF_BOUNDS")
                if o_orphan_cust.loc[idx]: r.append("ORPHAN_CUSTOMER_ID")
                if o_orphan_prod.loc[idx]: r.append("ORPHAN_PRODUCT_ID")
                if o_orphan_store.loc[idx]: r.append("ORPHAN_STORE_ID")
                reasons.append(";".join(r))
            df_bad["failure_reason"] = reasons

            df_good = df_ord[~fail_mask].copy()
            df_good["order_id"] = df_good["order_id"].astype(str).str.strip()
            df_good["customer_id"] = df_good["customer_id"].astype(str).str.strip()
            df_good["product_id"] = df_good["product_id"].astype(str).str.strip()
            df_good["store_id"] = df_good["store_id"].astype(str).str.strip()
            df_good["order_status"] = status_std[~fail_mask]
            df_good["payment_mode"] = pay_std[~fail_mask]
            df_good["order_ts"] = parsed_ts[~fail_mask]
            return df_good, df_bad

        df_o_clean, df_o_failed = validate_orders_df(df_o, is_increment=False)
        write_quarantine(df_o_failed, "orders", run_id)
        collect_rejections(df_o_failed, "orders.csv", "order_id")
        total_quarantined += len(df_o_failed)

        # Apply incremental load (upsert on order_id: overwrite status changes & append new orders)
        if load_increment and os.path.exists(inc_path):
            df_inc_clean, df_inc_failed = validate_orders_df(df_inc, is_increment=True)
            if not df_inc_failed.empty:
                write_quarantine(df_inc_failed, "orders_increment", run_id)
                collect_rejections(df_inc_failed, "orders_2026-10-01_increment.csv", "order_id")
                total_quarantined += len(df_inc_failed)
            # Upsert: concat and keep latest by order_id
            df_o_clean = pd.concat([df_o_clean, df_inc_clean], ignore_index=True)
            df_o_clean = df_o_clean.drop_duplicates(subset=["order_id"], keep="last")

        # --- E. PAYMENTS VALIDATION ---
        pay_dup = df_pay.duplicated(subset=["payment_id"], keep="first")
        pay_mode_std = df_pay["payment_mode"].apply(standardise_payment_mode)
        pay_bad_mode = ~pay_mode_std.isin(ALLOWED_PAY_MODES)
        pay_neg_amt = df_pay["amount_paid"].isna() | (df_pay["amount_paid"] <= 0)

        add_dq("payments_unique_id", "Uniqueness", "Critical", len(df_pay), pay_dup.sum())
        add_dq("payments_valid_mode", "Validity", "Critical", len(df_pay), pay_bad_mode.sum())
        add_dq("payments_positive_amount", "Accuracy", "Critical", len(df_pay), pay_neg_amt.sum())
        add_dq(
            "payments_order_exists",
            "Consistency",
            "Critical",
            len(df_pay),
            (~df_pay["order_id"].isin(df_o["order_id"])).sum(),
        )
        pay_bad_status = ~df_pay["payment_status"].astype(str).str.strip().str.upper().isin(
            ["SUCCESS", "FAILED", "REFUNDED"]
        )
        add_dq("payments_valid_status", "Validity", "Critical", len(df_pay), pay_bad_status.sum())

        pay_fail_mask = pay_dup | pay_bad_mode | pay_neg_amt | pay_bad_status
        df_pay_failed = df_pay[pay_fail_mask].copy()
        reasons_pay = []
        for idx in df_pay_failed.index:
            r = []
            if pay_dup.loc[idx]: r.append("DUPLICATE_PAYMENT_ID")
            if pay_bad_mode.loc[idx]: r.append("INVALID_PAYMENT_MODE")
            if pay_neg_amt.loc[idx]: r.append("NON_POSITIVE_AMOUNT_PAID")
            if pay_bad_status.loc[idx]: r.append("INVALID_PAYMENT_STATUS")
            reasons_pay.append(";".join(r))
        df_pay_failed["failure_reason"] = reasons_pay
        write_quarantine(df_pay_failed, "payments", run_id)
        collect_rejections(df_pay_failed, "payments.csv", "payment_id")
        total_quarantined += len(df_pay_failed)

        df_pay_clean = df_pay[~pay_fail_mask].copy()
        df_pay_clean["payment_id"] = df_pay_clean["payment_id"].astype(str).str.strip()
        df_pay_clean["order_id"] = df_pay_clean["order_id"].astype(str).str.strip()
        df_pay_clean["payment_mode"] = pay_mode_std[~pay_fail_mask]
        df_pay_clean["payment_status"] = df_pay_clean["payment_status"].astype(str).str.strip().str.upper()

        df_pay_reconciliation = df_pay.drop_duplicates(
            subset=["payment_id"], keep="first"
        ).copy()
        df_pay_reconciliation["payment_mode"] = pay_mode_std.loc[
            df_pay_reconciliation.index
        ]
        df_pay_reconciliation["payment_status"] = (
            df_pay_reconciliation["payment_status"].astype(str).str.strip().str.upper()
        )

        # Save all DQ check results to dq_results table
        save_dq_results(run_id, dag_id, dq_checks)

        # --- F. TRANSFORM: ENRICH FACT ORDERS ---
        df_fact = df_o_clean.merge(
            df_p_clean[["product_id", "category", "cost_price", "mrp"]],
            on="product_id",
            how="inner"
        ).merge(
            df_s_clean[["store_id", "store_name", "zone"]],
            on="store_id",
            how="inner"
        )

        df_fact["order_date"] = df_fact["order_ts"].dt.date
        df_fact["net_revenue"] = df_fact.apply(
            lambda r: round(r["order_amount"], 2) if r["order_status"] == "DELIVERED" else 0.0,
            axis=1
        )
        df_fact["gross_margin"] = df_fact.apply(
            lambda r: round(r["order_amount"] - (r["quantity"] * r["cost_price"]), 2)
            if r["order_status"] == "DELIVERED" else 0.0,
            axis=1
        )
        df_fact["within_15_min"] = df_fact.apply(
            lambda r: True if (r["order_status"] == "DELIVERED" and pd.notna(r["delivery_minutes"]) and r["delivery_minutes"] <= 15) else False,
            axis=1
        )

        # --- G. LOAD IDEMPOTENT STAR SCHEMA INTO DUCKDB ---
        init_db()
        with warehouse_connection() as con:
            con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM df_c_clean")
            con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM df_p_clean")
            con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM df_s_clean")
            con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM df_fact")
            con.execute("CREATE OR REPLACE TABLE etl_clean_payments AS SELECT * FROM df_pay_clean")
            con.execute(
                "CREATE OR REPLACE TABLE etl_recon_payments AS "
                "SELECT * FROM df_pay_reconciliation"
            )
            rejection_df = (
                pd.concat(rejection_frames, ignore_index=True)
                if rejection_frames
                else pd.DataFrame(columns=["source_file", "row_key", "failure_reason"])
            )
            con.execute("CREATE OR REPLACE TABLE etl_rejections AS SELECT * FROM rejection_df")

        total_extracted = (
            c_info["rows"] + p_info["rows"] + s_info["rows"] + o_info["rows"] + pay_info["rows"] + inc_rows
        )
        total_loaded = len(df_c_clean) + len(df_p_clean) + len(df_s_clean) + len(df_fact)

        return {
            "start_time": start_ts,
            "rows_extracted": int(total_extracted),
            "rows_loaded": int(total_loaded),
            "rows_quarantined": int(total_quarantined),
            "fact_orders_count": len(df_fact),
        }

    # 6. RECONCILE ORDERS WITH PAYMENTS
    @task
    def reconcile_payments(load_summary):
        with warehouse_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE etl_payment_reconciliation AS
                WITH ord AS (
                    SELECT order_id, order_date, order_status, payment_mode AS order_payment_mode, order_amount
                    FROM etl_fact_orders
                ),
                pay AS (
                    SELECT payment_id, order_id, payment_mode AS gateway_payment_mode, amount_paid, payment_status
                    FROM etl_recon_payments
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
                        WHEN o.order_id IS NULL THEN 'ORPHAN_PAYMENT_NO_VALID_ORDER'
                        WHEN p.payment_id IS NULL AND o.order_status = 'DELIVERED' THEN 'DELIVERED_ORDER_MISSING_PAYMENT'
                        WHEN o.order_status = 'DELIVERED' AND p.payment_status != 'SUCCESS' THEN 'DELIVERED_ORDER_PAYMENT_NOT_SUCCESS'
                        WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN 'CANCELLED_ORDER_CHARGED'
                        WHEN o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS' THEN 'RETURNED_ORDER_NOT_REFUNDED'
                        WHEN p.payment_status = 'SUCCESS' AND ABS(o.order_amount - p.amount_paid) > 0.05 THEN 'SUCCESS_PAYMENT_AMOUNT_MISMATCH'
                        ELSE 'RECONCILED'
                    END AS mismatch_reason
                FROM ord o
                FULL OUTER JOIN pay p ON o.order_id = p.order_id
                WHERE
                    o.order_id IS NULL
                    OR (p.payment_id IS NULL AND o.order_status = 'DELIVERED')
                    OR (o.order_status = 'DELIVERED' AND p.payment_status != 'SUCCESS')
                    OR (o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS')
                    OR (o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS')
                    OR (p.payment_status = 'SUCCESS' AND ABS(o.order_amount - p.amount_paid) > 0.05);
            """)
            mismatch_cnt = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
        print(f"Total payment reconciliation mismatches identified: {mismatch_cnt}")
        return {"mismatches": int(mismatch_cnt)}

    # 7. REPORT GENERATION
    @task
    def build_reports(load_summary, recon_summary):
        with warehouse_connection() as con:
            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT
                    order_date,
                    zone,
                    store_id,
                    store_name,
                    COUNT(order_id) AS total_orders,
                    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
                    ROUND(SUM(net_revenue), 2) AS daily_net_revenue
                FROM etl_fact_orders
                GROUP BY 1, 2, 3, 4
                ORDER BY 1, 2, 3;

                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT
                    category,
                    COUNT(CASE WHEN order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
                    ROUND(SUM(net_revenue), 2) AS total_revenue,
                    ROUND(SUM(gross_margin), 2) AS total_gross_margin,
                    ROUND(SUM(gross_margin) * 100.0 / NULLIF(SUM(net_revenue), 0), 2) AS margin_pct
                FROM etl_fact_orders
                GROUP BY 1
                ORDER BY total_revenue DESC;

                CREATE OR REPLACE TABLE etl_delivery_sla AS
                SELECT store_id, store_name,
                       ROUND(AVG(delivery_minutes), 2) AS average_delivery_minutes,
                       COUNT(*) AS delivered_orders,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE within_15_min) /
                             NULLIF(COUNT(*), 0), 2) AS within_15_minutes_pct
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY store_id, store_name;

                CREATE OR REPLACE TABLE etl_top_customers AS
                SELECT f.customer_id, c.loyalty_tier,
                       ROUND(SUM(f.net_revenue), 2) AS total_spend,
                       RANK() OVER (ORDER BY SUM(f.net_revenue) DESC) AS spend_rank
                FROM etl_fact_orders f
                JOIN etl_dim_customer c USING (customer_id)
                WHERE f.order_status = 'DELIVERED'
                GROUP BY f.customer_id, c.loyalty_tier
                QUALIFY spend_rank <= 10;

                CREATE OR REPLACE TABLE etl_payment_mode_revenue AS
                SELECT payment_mode,
                       ROUND(SUM(net_revenue), 2) AS net_revenue
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY payment_mode
                ORDER BY net_revenue DESC;
            """)

            print("\n=== ETL CATEGORY MARGIN REPORT ===")
            print(con.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))

            print("\n=== ETL ZONE REVENUE SUMMARY ===")
            print(con.execute("""
                SELECT zone, SUM(delivered_orders) AS delivered_orders, ROUND(SUM(daily_net_revenue), 2) AS net_revenue
                FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY net_revenue DESC
            """).df().to_string(index=False))
        return "REPORTS_BUILT"

    # 8. RUN AUDIT (Part F Observability)
    @task(trigger_rule="all_done")
    def record_pipeline_run(load_summary, report_status, **context):
        init_db()
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        end_ts = datetime.utcnow()

        if load_summary and isinstance(load_summary, dict):
            start_ts = datetime.fromisoformat(load_summary["start_time"])
            extracted = load_summary["rows_extracted"]
            loaded = load_summary["rows_loaded"]
            quarantined = load_summary["rows_quarantined"]
            status = "SUCCESS" if report_status == "REPORTS_BUILT" else "FAILED"
        else:
            start_ts = end_ts
            extracted, loaded, quarantined = 0, 0, 0
            status = "FAILED"

        with warehouse_connection() as con:
            con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [str(run_id), str(dag_id)])
            con.execute("""
                INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [str(run_id), str(dag_id), start_ts, end_ts, extracted, loaded, quarantined, status])
        print(f"Recorded pipeline run {run_id} with status={status}")
        if status != "SUCCESS":
            raise AirflowException(f"ETL run {run_id} failed; inspect task states and pipeline_runs.")

    # DAG Wiring
    c = extract_customers()
    p = extract_products()
    s = extract_stores()
    o = extract_orders()
    pay = extract_payments()

    summary = validate_transform_load(c, p, s, o, pay)
    recon = reconcile_payments(summary)
    rep = build_reports(summary, recon)
    record_pipeline_run(summary, rep)


nammamart_etl_pipeline()