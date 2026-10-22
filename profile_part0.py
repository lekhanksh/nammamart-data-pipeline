import pandas as pd
from pathlib import Path

DATA_DIR = "include/data/nammamart"
files = {
    "customers": ("customers.csv", "customer_id"),
    "products": ("products.csv", "product_id"),
    "stores": ("stores.csv", "store_id"),
    "orders": ("orders.csv", "order_id"),
    "payments": ("payments.csv", "payment_id"),
    "orders_inc": ("orders_2026-10-01_increment.csv", "order_id"),
}

dfs = {k: pd.read_csv(f"{DATA_DIR}/{v[0]}") for k, v in files.items()}

print("==========================================")
print("1. ROW COUNTS, NULLS & DUPLICATE KEYS")
print("==========================================")
for name, (fname, pk) in files.items():
    df = dfs[name]
    nulls = df.isnull().sum()
    null_dict = nulls[nulls > 0].to_dict()
    dup_keys = df[df.duplicated(subset=[pk], keep=False)][pk].unique().tolist()
    print(f"\n📂 {fname} ({len(df)} rows)")
    print(f"   - Nulls per column: {null_dict if null_dict else 'None'}")
    print(f"   - Duplicate {pk}s ({len(dup_keys)}): {dup_keys[:10]}")

print("\n==========================================")
print("2. DISTINCT CATEGORY VALUES (SPOT TYPOS)")
print("==========================================")
print("orders.order_status:", dfs["orders"]["order_status"].unique().tolist())
print("orders.payment_mode:", dfs["orders"]["payment_mode"].unique().tolist())
print("payments.payment_mode:", dfs["payments"]["payment_mode"].unique().tolist())
print("payments.payment_status:", dfs["payments"]["payment_status"].unique().tolist())
print("customers.loyalty_tier:", dfs["customers"]["loyalty_tier"].unique().tolist())
print("stores.zone:", dfs["stores"]["zone"].unique().tolist())
print("products.category:", dfs["products"]["category"].unique().tolist())

print("\n==========================================")
print("3. NUMERIC & DATE MIN/MAX")
print("==========================================")
for name, df in dfs.items():
    num_cols = df.select_dtypes(include="number").columns
    print(f"\n📊 {name} numeric min/max:")
    for c in num_cols:
        print(f"   - {c}: min={df[c].min()}, max={df[c].max()}")
    date_cols = [
        c for c in df.columns
        if any(token in c.lower() for token in ("date", "_ts", "timestamp"))
    ]
    print(f"📅 {name} date min/max:")
    for c in date_cols:
        parsed = pd.to_datetime(df[c], errors="coerce")
        print(f"   - {c}: min={parsed.min()}, max={parsed.max()}")

print("\n==========================================")
print("4. DETAILED ISSUES LOG (BY FILE & ROW KEY)")
print("==========================================")
c, p, s, o, pay, inc = dfs["customers"], dfs["products"], dfs["stores"], dfs["orders"], dfs["payments"], dfs["orders_inc"]

# Customers checks
print("\n--- CUSTOMERS ISSUES ---")
print("Null email/phone/name:", c[c[["full_name", "email", "phone"]].isnull().any(axis=1)]["customer_id"].tolist())
print("Invalid email (no @):", c[~c["email"].astype(str).str.contains("@", na=False)]["customer_id"].tolist())
print("Invalid phone (not 10 digits):", c[~c["phone"].astype(str).str.replace(r"\D", "", regex=True).str.len().eq(10)]["customer_id"].tolist())
print("Future signup_date (>2026-10-01):", c[c["signup_date"].astype(str) > "2026-10-01"]["customer_id"].tolist())

# Products checks
print("\n--- PRODUCTS ISSUES ---")
print("Null category/price:", p[p[["category", "mrp", "cost_price"]].isnull().any(axis=1)]["product_id"].tolist())
print("mrp <= 0 or cost_price <= 0:", p[(p["mrp"] <= 0) | (p["cost_price"] <= 0)]["product_id"].tolist())
print("cost_price > mrp:", p[p["cost_price"] > p["mrp"]]["product_id"].tolist())

# Stores checks
print("\n--- STORES ISSUES ---")
print("All stores rows with any anomaly:")
print(s[s.isnull().any(axis=1) | (s["capacity_orders_per_day"] <= 0) | (~s["pincode"].astype(str).str.len().eq(6))][["store_id", "store_name", "zone", "pincode", "capacity_orders_per_day"]])

# Orders checks
print("\n--- ORDERS ISSUES ---")
print("Orphan customer_id:", o[~o["customer_id"].isin(c["customer_id"])]["order_id"].tolist())
print("Orphan product_id:", o[~o["product_id"].isin(p["product_id"])]["order_id"].tolist())
print("Orphan store_id:", o[~o["store_id"].isin(s["store_id"])]["order_id"].tolist())
print("Increment orphan customer_id:", inc[~inc["customer_id"].isin(c["customer_id"])]["order_id"].tolist())
print("Increment orphan product_id:", inc[~inc["product_id"].isin(p["product_id"])]["order_id"].tolist())
print("Increment orphan store_id:", inc[~inc["store_id"].isin(s["store_id"])]["order_id"].tolist())
print("Quantity <= 0:", o[o["quantity"] <= 0]["order_id"].tolist())
print("Discount outside 0-100:", o[(o["discount_pct"] < 0) | (o["discount_pct"] > 100)]["order_id"].tolist())
calc = (o["quantity"] * o["unit_price"] * (1 - o["discount_pct"].fillna(0) / 100.0)).round(2)
print("Formula mismatch (order_amount):", o[(o["order_amount"] - calc).abs() > 0.05]["order_id"].tolist())
deliv = o[o["order_status"].astype(str).str.strip().str.upper() == "DELIVERED"]
print("DELIVERED with null delivery_minutes:", deliv[deliv["delivery_minutes"].isnull()]["order_id"].tolist())
print("DELIVERED with delivery_minutes >= 120 or <= 0:", deliv[(deliv["delivery_minutes"] >= 120) | (deliv["delivery_minutes"] <= 0)]["order_id"].tolist())
print("Future order_ts (>2026-10-01):", o[o["order_ts"].astype(str) > "2026-10-01"]["order_id"].tolist())

# Payments checks
print("\n--- PAYMENTS ISSUES ---")
print("Orphan order_id in payments:", pay[~pay["order_id"].isin(o["order_id"])]["payment_id"].tolist())
print("Negative/zero amount_paid:", pay[pay["amount_paid"] <= 0]["payment_id"].tolist())
m = pay.merge(o[["order_id", "order_amount", "order_status"]], on="order_id", how="inner")
succ_mismatch = m[(m["payment_status"].astype(str).str.strip().str.upper() == "SUCCESS") & ((m["amount_paid"] - m["order_amount"]).abs() > 0.05)]
print("SUCCESS payment != order_amount:", succ_mismatch["payment_id"].tolist())
print("SUCCESS payments with no matching order:", pay[
    (pay["payment_status"].astype(str).str.strip().str.upper() == "SUCCESS")
    & (~pay["order_id"].isin(o["order_id"]))
]["payment_id"].tolist())

# Save a row-level, PII-free issue log for the written submission.
issue_rows = []


def log_issues(df, mask, file_name, key, column, problem, dimension, rule, severity):
    for idx in df.index[mask.fillna(False)]:
        value = df.at[idx, key]
        issue_rows.append({
            "file": file_name,
            "row_key": "<NULL>" if pd.isna(value) else str(value),
            "column": column,
            "problem": problem,
            "quality_dimension": dimension,
            "proposed_rule": rule,
            "severity": severity,
        })


def normalized_payment_mode(series):
    mapping = {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }
    return series.fillna("").astype(str).str.strip().str.lower().map(mapping).fillna(
        series.fillna("").astype(str).str.strip()
    )


def collect_customer_issues(df, file_name):
    log_issues(df, df.duplicated("customer_id"), file_name, "customer_id", "customer_id",
               "Duplicate customer key", "Uniqueness", "Keep one deterministic customer row; quarantine duplicates", "Critical")
    log_issues(df, df["email"].isna(), file_name, "customer_id", "email",
               "Email is missing", "Completeness", "Retain customer but mask missing email", "Warning")
    email_bad = ~df["email"].fillna("").astype(str).str.contains("@", na=False)
    log_issues(df, email_bad & df["email"].notna(), file_name, "customer_id", "email",
               "Email has no @ sign", "Validity", "Mask invalid email and report it", "Warning")
    phone_digits = df["phone"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.replace(r"\D", "", regex=True)
    log_issues(df, phone_digits.str.len().ne(10), file_name, "customer_id", "phone",
               "Phone is not 10 digits", "Validity", "Mask invalid phone and report it", "Warning")
    log_issues(df, df["signup_date"].astype(str).gt("2026-10-01"), file_name, "customer_id", "signup_date",
               "Signup date is after the source cutoff", "Timeliness", "Flag dates after 2026-10-01", "Warning")
    tier = df["loyalty_tier"].fillna("").astype(str).str.strip()
    normalized_tier = tier.str.title()
    log_issues(df, tier.ne(normalized_tier), file_name, "customer_id", "loyalty_tier",
               "Loyalty tier casing is inconsistent", "Consistency", "Trim and standardize title casing", "Warning")


def collect_order_issues(df, file_name):
    log_issues(df, df.duplicated("order_id"), file_name, "order_id", "order_id",
               "Duplicate order key", "Uniqueness", "Keep one deterministic row per order_id", "Critical")
    log_issues(df, df["customer_id"].isna(), file_name, "order_id", "customer_id",
               "Customer key is missing", "Completeness", "Reject rows without customer_id", "Critical")
    parsed_ts = pd.to_datetime(df["order_ts"], errors="coerce")
    log_issues(df, parsed_ts.isna(), file_name, "order_id", "order_ts",
               "Order timestamp is missing or invalid", "Completeness", "Require a parseable timestamp", "Critical")
    log_issues(df, parsed_ts.gt(pd.Timestamp("2026-10-01 23:59:59")), file_name, "order_id", "order_ts",
               "Order timestamp is after source cutoff", "Timeliness", "Reject rows after 2026-10-01 23:59:59", "Critical")
    status = df["order_status"].fillna("").astype(str).str.strip()
    status_upper = status.str.upper()
    log_issues(df, ~status_upper.isin({"DELIVERED", "CANCELLED", "RETURNED"}), file_name, "order_id", "order_status",
               "Order status is not in allowed list", "Validity", "Allow DELIVERED/CANCELLED/RETURNED only", "Critical")
    log_issues(df, status.ne(status_upper) & status_upper.isin({"DELIVERED", "CANCELLED", "RETURNED"}),
               file_name, "order_id", "order_status", "Order status casing needs standardization",
               "Consistency", "Trim and uppercase allowed values", "Warning")
    pay_mode = normalized_payment_mode(df["payment_mode"])
    log_issues(df, ~pay_mode.isin({"UPI", "Card", "Cash on Delivery", "Wallet"}), file_name,
               "order_id", "payment_mode", "Payment mode is not recognized", "Validity",
               "Map known aliases and reject unknown values", "Critical")
    log_issues(df, pd.to_numeric(df["quantity"], errors="coerce").le(0), file_name, "order_id", "quantity",
               "Quantity is zero, negative, or invalid", "Accuracy", "Require quantity > 0", "Critical")
    discount = pd.to_numeric(df["discount_pct"], errors="coerce")
    log_issues(df, discount.lt(0) | discount.gt(100) | discount.isna(), file_name, "order_id", "discount_pct",
               "Discount is outside 0-100 or invalid", "Validity", "Require 0 <= discount_pct <= 100", "Critical")
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    amount = pd.to_numeric(df["order_amount"], errors="coerce")
    expected = (qty * price * (1 - discount.fillna(0) / 100)).round(2)
    mismatch = amount.isna() | expected.isna() | (amount - expected).abs().gt(0.05)
    log_issues(df, mismatch, file_name, "order_id", "order_amount",
               "Order amount disagrees with pricing formula", "Accuracy", "Recompute and compare within 0.05", "Critical")
    delivered = status_upper.eq("DELIVERED")
    delivery = pd.to_numeric(df["delivery_minutes"], errors="coerce")
    log_issues(df, delivered & delivery.isna(), file_name, "order_id", "delivery_minutes",
               "Delivered order has no delivery duration", "Completeness", "Require delivery_minutes for DELIVERED", "Critical")
    log_issues(df, delivered & delivery.notna() & ((delivery <= 0) | (delivery >= 120)),
               file_name, "order_id", "delivery_minutes", "Delivered time is outside valid bounds",
               "Accuracy", "Require 0 < delivery_minutes < 120", "Critical")
    for fk, master, dimension in (
        ("customer_id", c, "customer"),
        ("product_id", p, "product"),
        ("store_id", s, "store"),
    ):
        log_issues(df, df[fk].notna() & ~df[fk].isin(master[fk]),
            file_name, "order_id", fk, f"Referenced {dimension} key is missing",
            "Consistency", f"Require {fk} to exist in {dimension} master", "Critical")


collect_customer_issues(c, "customers.csv")
log_issues(p, p.duplicated("product_id"), "products.csv", "product_id", "product_id",
           "Duplicate product key", "Uniqueness", "Keep one deterministic product row", "Critical")
log_issues(p, p["category"].isna(), "products.csv", "product_id", "category",
           "Product category is missing", "Completeness", "Quarantine until category is assigned", "Critical")
log_issues(p, p["mrp"].le(0) | p["cost_price"].le(0), "products.csv", "product_id", "mrp/cost_price",
           "MRP or cost price is not positive", "Accuracy", "Require both prices > 0", "Critical")
log_issues(p, p["cost_price"].gt(p["mrp"]), "products.csv", "product_id", "cost_price",
           "Cost price exceeds MRP", "Accuracy", "Review or quarantine negative-margin master data", "Critical")
log_issues(s, s["zone"].isna(), "stores.csv", "store_id", "zone",
           "Store zone is missing", "Completeness", "Repair with an approved zone; current pipeline imputes South", "Warning")
log_issues(s, s["capacity_orders_per_day"].le(0), "stores.csv", "store_id", "capacity_orders_per_day",
           "Store capacity is not positive", "Accuracy", "Require positive daily capacity", "Warning")
log_issues(s, ~s["pincode"].astype(str).str.len().eq(6), "stores.csv", "store_id", "pincode",
           "Pincode is not six digits", "Validity", "Require a six-digit pincode", "Warning")
collect_order_issues(o, "orders.csv")
collect_order_issues(inc, "orders_2026-10-01_increment.csv")

log_issues(pay, pay.duplicated("payment_id"), "payments.csv", "payment_id", "payment_id",
           "Duplicate payment key", "Uniqueness", "Keep one deterministic payment row", "Critical")
log_issues(pay, ~pay["order_id"].isin(o["order_id"]), "payments.csv", "payment_id", "order_id",
           "Payment references unknown order", "Consistency", "Retain payment and report as orphan in reconciliation", "Critical")
log_issues(pay, pd.to_numeric(pay["amount_paid"], errors="coerce").le(0), "payments.csv", "payment_id", "amount_paid",
           "Payment amount is zero, negative, or invalid", "Accuracy", "Require amount_paid > 0", "Critical")
pay_mode = normalized_payment_mode(pay["payment_mode"])
log_issues(pay, ~pay_mode.isin({"UPI", "Card", "Cash on Delivery", "Wallet"}), "payments.csv",
           "payment_id", "payment_mode", "Payment mode is not recognized", "Validity",
           "Map known aliases and reject unknown values", "Critical")
pay_status = pay["payment_status"].fillna("").astype(str).str.strip().str.upper()
log_issues(pay, ~pay_status.isin({"SUCCESS", "FAILED", "REFUNDED"}), "payments.csv",
           "payment_id", "payment_status", "Payment status is not recognized", "Validity",
           "Allow SUCCESS/FAILED/REFUNDED", "Critical")
joined_payments = pay.merge(o[["order_id", "order_amount"]], on="order_id", how="left")
paid_amount = pd.to_numeric(joined_payments["amount_paid"], errors="coerce")
order_amount = pd.to_numeric(joined_payments["order_amount"], errors="coerce")
joined_status = joined_payments["payment_status"].fillna("").astype(str).str.strip().str.upper()
log_issues(joined_payments, joined_status.eq("SUCCESS") & order_amount.notna()
           & (paid_amount - order_amount).abs().gt(0.05), "payments.csv", "payment_id", "amount_paid",
           "SUCCESS amount does not match order amount", "Consistency",
           "Reconcile successful payment to order within 0.05", "Critical")
log_issues(pay, pay["gateway_ref"].isna(), "payments.csv", "payment_id", "gateway_ref",
           "Gateway reference is missing", "Completeness", "Retain but flag missing gateway reference", "Warning")

issues_path = Path("assignment/data_issues_log.csv")
pd.DataFrame(issue_rows).to_csv(issues_path, index=False)
print(f"\nRow-level issue log written to {issues_path} ({len(issue_rows)} issue records).")
print(pd.DataFrame(issue_rows).groupby(["file", "quality_dimension"]).size().to_string())