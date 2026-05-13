import json  # noqa: F401
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook


REPO_ROOT = Path(__file__).parent.parent
raw_sales_asset = Asset("raw_sales_starter")


@dag(
    dag_id="02_daily_sales_starter",
    start_date=datetime(2026, 4, 30),
    schedule="@daily",
    catchup=True,
    tags=["starter"],
)
def daily_sales():

    # TODO 1: Add a task that prints logical_date(ds) and timestamp(ts)

    # TODO 2: Add a branch operator that checks if file exists. If it does, continue to insert_sales. If not, skip to the end.    

    # ignore this outlet for now, it's covered in next module
    @task(outlets=[raw_sales_asset])
    def insert_sales(ds=None, **context):
        # TODO 3: Build the file path using the `ds` variable (logical date, format YYYY-MM-DD).
        # The sales files live at: REPO_ROOT / "data" / "sales" / "<date>.json"
        # Load and return the JSON content as a Python list.
        records = []

        hook = PostgresHook(postgres_conn_id="bookshop_postgres")
        hook.run("DELETE FROM raw_sales WHERE sale_date = %s", parameters=[ds])
        rows = [(rec["isbn"], ds, rec["quantity"], rec["total"]) for rec in records]
        hook.insert_rows(
            table="raw_sales",
            rows=rows,
            target_fields=["isbn", "sale_date", "quantity", "total"],
        )

        # ignore this piece of code for now
        # outlet_events is a preview of the Assets pattern taught in Exercise 3a.
        # This attaches metadata to the raw_sales event so DAG 03b knows which date to validate.
        # For now, treat it as: "DAG 03b will wake up when this task completes and can read this dict."
        context["outlet_events"][raw_sales_asset].extra = {"date": ds, "count": len(records)}
        
        
        # return values become xcom of this task
        return {
            "date": ds,
            "count": len(records),
        }

    @task
    def log_summary(summary_dict):
        # TODO 4: Print a summary line showing the date and the number of records inserted.
        # Hint: `count` is the return value from insert_sales, passed via XCom automatically.
        print(f"Date: {summary_dict['date']} | Inserted: {summary_dict['count']} records into raw_sales")

    count = insert_sales()
    log_summary(count)


daily_sales()
