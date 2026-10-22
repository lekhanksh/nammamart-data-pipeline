import os
import re
import fcntl
from contextlib import contextmanager
from datetime import datetime
import pandas as pd
import duckdb

# Paths inside the Astro Airflow Docker container (with local fallback)
BASE_DIR = "/usr/local/airflow" if os.path.exists("/usr/local/airflow") else "."
DB_PATH = f"{BASE_DIR}/include/warehouse_lekhanksh.duckdb"
DATA_DIR = f"{BASE_DIR}/include/data/nammamart"
QUARANTINE_DIR = f"{BASE_DIR}/include/quarantine/lekhanksh"
SQL_DIR = f"{BASE_DIR}/include/sql"


@contextmanager
def warehouse_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with open(f"{DB_PATH}.lock", "a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            con = duckdb.connect(DB_PATH)
            try:
                yield con
            finally:
                con.close()
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def mask_email(val):
    if pd.isna(val) or not isinstance(val, str) or "@" not in val.strip():
        return "invalid_email"
    val = val.strip()
    local, domain = val.split("@", 1)
    prefix = local[:2] if len(local) >= 2 else local[:1]
    return f"{prefix}***@{domain}"


def mask_phone(val):
    if pd.isna(val):
        return "invalid_phone"
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = re.sub(r"\D", "", s)
    if len(digits) == 10:
        return f"{digits[:2]}******{digits[-2:]}"
    return "invalid_phone"


def mask_name(val):
    if pd.isna(val) or not str(val).strip():
        return "unknown"
    return f"{str(val).strip()[0]}***"


def standardise_payment_mode(val):
    if pd.isna(val):
        return ""
    v = str(val).strip().lower()
    mapping = {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }
    return mapping.get(v, str(val).strip())


def init_db():
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    with warehouse_connection() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR,
                dag_id VARCHAR,
                check_name VARCHAR,
                dimension VARCHAR,
                severity VARCHAR,
                rows_checked INTEGER,
                rows_failed INTEGER,
                status VARCHAR,
                checked_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted INTEGER,
                rows_loaded INTEGER,
                rows_quarantined INTEGER,
                status VARCHAR
            );
        """)


def save_dq_results(run_id: str, dag_id: str, checks: list):
    init_db()
    df_dq = pd.DataFrame(checks)
    df_dq["run_id"] = str(run_id)
    df_dq["dag_id"] = str(dag_id)
    df_dq["checked_at"] = datetime.utcnow()
    with warehouse_connection() as con:
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [str(run_id), str(dag_id)])
        con.execute("""
            INSERT INTO dq_results
            SELECT run_id, dag_id, check_name, dimension, severity,
                   rows_checked, rows_failed, status, checked_at
            FROM df_dq
        """)


def write_quarantine(df_failed: pd.DataFrame, file_name: str, run_id: str) -> str:
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    safe_run = re.sub(r"[^a-zA-Z0-9_-]", "_", str(run_id))
    out_path = os.path.join(QUARANTINE_DIR, f"{file_name}_failed_{safe_run}.csv")
    df_failed.to_csv(out_path, index=False)
    return out_path