import json
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook


REPO_ROOT = Path(__file__).parent.parent.parent


@dag(
    dag_id="02_daily_sales",
    start_date=datetime(2026, 5, 5),
    schedule="@daily",
    catchup=True,
    tags=["solution"],
)
def daily_sales():
    @task
    def print_logical_date_and_ds(ds=None, ts=None):
        print(f"timestamp: {ts} | ds: {ds}")
    
    @task.branch
    def check_file(ds=None):
        path = REPO_ROOT / "data" / "sales" / f"{ds}.json"
        if path.exists():
            return "insert_sales"
        else:
            print(f"File not found: {path}. Skipping insert_sales.")
            return "no_file"

    @task(outlets=[Asset("raw_sales")])
    def insert_sales(ds=None, **context):
        path = REPO_ROOT / "data" / "sales" / f"{ds}.json"
        print(f"Loading file: {path}")
        records = json.loads(path.read_text())

        hook = PostgresHook(postgres_conn_id="bookshop_postgres")
        hook.run("DELETE FROM raw_sales WHERE sale_date = %s", parameters=[ds])
        rows = [(rec["isbn"], ds, rec["quantity"], rec["total"]) for rec in records]
        hook.insert_rows(
            table="raw_sales",
            rows=rows,
            target_fields=["isbn", "sale_date", "quantity", "total"],
        )
        print(f"Loaded {len(records)} records for {ds}")
        context["outlet_events"][Asset("raw_sales")].extra = {"date": ds, "count": len(records)}
        return {
            "date": ds,
            "count": len(records),
        }

    @task
    def log_summary(summary_dict):
        print(f"Date: {summary_dict['date']} | Inserted: {summary_dict['count']} records into raw_sales")


    @task
    def no_file():
        print("No file to process. Ending pipeline.")

    check_file_task = check_file()
    insert_sales_task = insert_sales()
    log_summary(insert_sales_task)
    date_print_task = print_logical_date_and_ds()

    date_print_task >> check_file_task >> [insert_sales_task, no_file()] 


daily_sales()
