import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.sdk import Asset
except ImportError:
    from airflow.datasets import Dataset as Asset

ORDERS_SUMMARY_ASSET = Asset("postgres://postgres:5432/airflow/public/daily_order_summary")


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
    dag_id="orders_report",
    schedule=[ORDERS_SUMMARY_ASSET],
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["exam", "report"],
)
def orders_report():
    @task
    def log_top_customers():
        hook = PostgresHook(postgres_conn_id="orders_db")
        rows = hook.get_records(
            """
            SELECT customer_id, SUM(amount) AS total_delivered
            FROM orders
            WHERE status = 'DELIVERED'
            GROUP BY customer_id
            ORDER BY total_delivered DESC
            LIMIT 3;
            """
        )
        logging.info("=== Top 3 Customers by Total Delivered Amount ===")
        for rank, (customer_id, total_delivered) in enumerate(rows, start=1):
            logging.info("%d. Customer %s: %.2f", rank, customer_id, float(total_delivered))

    log_top_customers()


orders_report()