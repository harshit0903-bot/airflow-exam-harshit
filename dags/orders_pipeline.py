import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.sdk import Asset
except ImportError:
    from airflow.datasets import Dataset as Asset

try:
    from airflow.providers.standard.sensors.filesystem import FileSensor
except ImportError:
    from airflow.sensors.filesystem import FileSensor

from utils.validation import validate_row

ORDERS_SUMMARY_ASSET = Asset("postgres://postgres:5432/airflow/public/daily_order_summary")
BASE_DATA_DIR = Path("/opt/airflow/include/data")


def log_failure_callback(context):
    ti = context.get("task_instance")
    dag_id = context["dag"].dag_id if context.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_date = context.get("ds", "unknown_date")
    logging.error(
        "TASK FAILED | DAG: %s | Task: %s | Run Date: %s",
        dag_id,
        task_id,
        run_date,
    )


default_args = {
    "owner": "harshit",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": log_failure_callback,
}


@dag(
    dag_id="orders_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["exam", "orders"],
)
def orders_pipeline():
    wait_for_landing = FileSensor(
        task_id="wait_for_landing_folder",
        fs_conn_id="fs_default",
        filepath="include/data/landing/{{ ds }}",
        mode="reschedule",
        poke_interval=10,
        timeout=300,
    )

    @task
    def list_csv_files() -> list[str]:
        ds = get_current_context()["ds"]
        landing_dir = BASE_DATA_DIR / "landing" / ds
        return sorted(str(p) for p in landing_dir.glob("*.csv"))

    @task
    def validate_and_clean_csv(file_path: str) -> dict:
        ds = get_current_context()["ds"]
        src = Path(file_path)
        valid_rows = []
        rejected_rows = []

        with src.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned, reason = validate_row(row)
                if cleaned is not None:
                    valid_rows.append(cleaned)
                else:
                    rejected_entry = dict(row)
                    rejected_entry["rejection_reason"] = reason
                    rejected_rows.append(rejected_entry)

        if rejected_rows:
            rej_dir = BASE_DATA_DIR / "rejected" / ds
            rej_dir.mkdir(parents=True, exist_ok=True)
            rej_file = rej_dir / f"{src.stem}_rejected.csv"
            fieldnames = list(rejected_rows[0].keys())
            with rej_file.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rejected_rows)

        return {"valid": valid_rows, "rejected_count": len(rejected_rows)}

    @task
    def load_orders(mapped_results: list[dict]) -> dict:
        unique_orders = {}
        total_rejected = 0

        for res in mapped_results:
            total_rejected += res["rejected_count"]
            for row in res["valid"]:
                oid = row["order_id"]
                if oid not in unique_orders:
                    unique_orders[oid] = row

        hook = PostgresHook(postgres_conn_id="orders_db")
        upsert_sql = """
            INSERT INTO orders (order_id, customer_id, order_date, amount, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date  = EXCLUDED.order_date,
                amount      = EXCLUDED.amount,
                status      = EXCLUDED.status;
        """

        conn = hook.get_conn()
        with conn.cursor() as cur:
            for o in unique_orders.values():
                cur.execute(
                    upsert_sql,
                    (o["order_id"], o["customer_id"], o["order_date"], o["amount"], o["status"]),
                )
        conn.commit()
        conn.close()

        delivered_amount = round(
            sum(o["amount"] for o in unique_orders.values() if o["status"] == "DELIVERED"),
            2,
        )
        return {
            "order_count": len(unique_orders),
            "delivered_amount": delivered_amount,
            "rejected_rows": total_rejected,
        }

    @task(outlets=[ORDERS_SUMMARY_ASSET])
    def write_daily_summary(stats: dict) -> None:
        ds = get_current_context()["ds"]
        hook = PostgresHook(postgres_conn_id="orders_db")
        sql = """
            INSERT INTO daily_order_summary (summary_date, order_count, delivered_amount, rejected_rows)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (summary_date) DO UPDATE SET
                order_count      = EXCLUDED.order_count,
                delivered_amount = EXCLUDED.delivered_amount,
                rejected_rows    = EXCLUDED.rejected_rows;
        """
        hook.run(
            sql,
            parameters=(
                ds,
                stats["order_count"],
                stats["delivered_amount"],
                stats["rejected_rows"],
            ),
        )

    files = list_csv_files()
    wait_for_landing >> files
    cleaned = validate_and_clean_csv.expand(file_path=files)
    stats = load_orders(cleaned)
    write_daily_summary(stats)


orders_pipeline()