import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from include.nammamart_helpers import DATA_DIR, SQL_DIR, warehouse_connection

DAG_ID = "nammamart_elt_lekhanksh"
SOURCE_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders": "orders.csv",
    "payments": "payments.csv",
}

DEFAULT_ARGS = {
    "owner": "lekhanksh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: immutable raw ingestion, SQL staging and reporting marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, 9, 0, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "fde"],
)
def nammamart_elt_pipeline():
    @task
    def load_raw_table(table_name: str, file_name: str) -> int:
        path = os.path.join(DATA_DIR, file_name)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Required source file does not exist: {path}")
        context = get_current_context()
        run_id = str(context["run_id"])
        with warehouse_connection() as con:
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS raw_{table_name} AS
                SELECT *, CAST(NULL AS TIMESTAMP) AS _loaded_at,
                          CAST(NULL AS VARCHAR) AS _source_file,
                          CAST(NULL AS VARCHAR) AS _run_id,
                          CAST(NULL AS BIGINT) AS _source_row
                FROM read_csv(?, all_varchar=true, nullstr='')
                WHERE FALSE
                """,
                [path],
            )
            con.execute(
                f"""
                INSERT INTO raw_{table_name}
                SELECT source.*, CURRENT_TIMESTAMP, ?, ?,
                       ROW_NUMBER() OVER ()
                FROM read_csv(?, all_varchar=true, nullstr='') AS source
                WHERE NOT EXISTS (
                    SELECT 1 FROM raw_{table_name}
                    WHERE _run_id = ? AND _source_file = ?
                )
                """,
                [file_name, run_id, path, run_id, file_name],
            )
            count = con.execute(
                "SELECT COUNT(*) FROM read_csv(?, all_varchar=true, nullstr='')",
                [path],
            ).fetchone()[0]

            if table_name == "orders":
                increment_name = "orders_2026-10-01_increment.csv"
                increment_path = os.path.join(DATA_DIR, increment_name)
                if os.path.isfile(increment_path):
                    con.execute(
                        """
                        INSERT INTO raw_orders
                        SELECT source.*, CURRENT_TIMESTAMP, ?, ?,
                               ROW_NUMBER() OVER ()
                        FROM read_csv(?, all_varchar=true, nullstr='') AS source
                        WHERE NOT EXISTS (
                            SELECT 1 FROM raw_orders
                            WHERE _run_id = ? AND _source_file = ?
                        )
                        """,
                        [increment_name, run_id, increment_path, run_id, increment_name],
                    )
                    count += con.execute(
                        "SELECT COUNT(*) FROM read_csv(?, all_varchar=true, nullstr='')",
                        [increment_path],
                    ).fetchone()[0]
        return int(count)

    @task
    def build_staging(_raw_counts: list[int]) -> None:
        from include.nammamart_helpers import init_db

        init_db()
        run_id = str(get_current_context()["run_id"]).replace("'", "''")
        with warehouse_connection() as con:
            with open(
                os.path.join(SQL_DIR, "nammamart_staging.sql"), encoding="utf-8"
            ) as sql_file:
                sql = sql_file.read().replace("{{RUN_ID}}", run_id)
            con.execute(sql)

    @task
    def build_marts(_staging_done: None) -> bool:
        with warehouse_connection() as con:
            with open(
                os.path.join(SQL_DIR, "nammamart_marts.sql"), encoding="utf-8"
            ) as sql_file:
                con.execute(sql_file.read())
            for table in (
                "mart_daily_store_kpis",
                "mart_category_margin",
                "mart_delivery_sla",
                "mart_payment_reconciliation",
                "mart_payment_mode_revenue",
                "mart_top_customers",
                "mart_rejections",
            ):
                print(f"\n=== {table} ===")
                print(con.execute(f"SELECT * FROM {table}").df().to_string(index=False))
        return True

    @task(trigger_rule="all_done")
    def record_run(_mart_done=None, **context) -> None:
        from include.nammamart_helpers import init_db

        init_db()
        run_id = str(context["run_id"])
        dag_run = context["dag_run"]
        end_time = pendulum.now("UTC").naive()
        start_time = (dag_run.start_date or dag_run.logical_date).replace(tzinfo=None)

        failed = _mart_done is not True
        with warehouse_connection() as con:
            extracted = con.execute(
                "SELECT COUNT(*) FROM raw_orders WHERE _run_id = ? "
                "UNION ALL SELECT COUNT(*) FROM raw_customers WHERE _run_id = ? "
                "UNION ALL SELECT COUNT(*) FROM raw_products WHERE _run_id = ? "
                "UNION ALL SELECT COUNT(*) FROM raw_stores WHERE _run_id = ? "
                "UNION ALL SELECT COUNT(*) FROM raw_payments WHERE _run_id = ?",
                [run_id] * 5,
            ).fetchall()
            rows_extracted = sum(row[0] for row in extracted)
            available_tables = {
                row[0]
                for row in con.execute("SHOW TABLES").fetchall()
            }
            rows_loaded = (
                con.execute(
                    "SELECT COUNT(*) FROM stg_orders "
                    "WHERE is_current AND dq_status <> 'FAIL'"
                ).fetchone()[0]
                if "stg_orders" in available_tables
                else 0
            )
            rows_quarantined = []
            for table_name in (
                "stg_orders",
                "stg_customers",
                "stg_products",
                "stg_stores",
                "stg_payments",
            ):
                if table_name in available_tables:
                    rows_quarantined.append(
                        con.execute(
                            f"SELECT COUNT(*) FROM {table_name} WHERE dq_status = 'FAIL'"
                        ).fetchone()
                    )
            con.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [run_id, DAG_ID],
            )
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    DAG_ID,
                    start_time,
                    end_time,
                    rows_extracted,
                    rows_loaded,
                    sum(row[0] for row in rows_quarantined),
                    "FAILED" if failed else "SUCCESS",
                ],
            )
        if failed:
            raise AirflowException(f"ELT run {run_id} failed; inspect task states and pipeline_runs.")

    raw_tasks = [
        load_raw_table.override(task_id=f"load_raw_{table}")(table, file_name)
        for table, file_name in SOURCE_FILES.items()
    ]
    staging = build_staging(raw_tasks)
    marts = build_marts(staging)
    audit = record_run(marts)
    for raw_task in raw_tasks:
        raw_task >> audit


nammamart_elt_pipeline()
