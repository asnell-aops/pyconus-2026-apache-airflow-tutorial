from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

REPO_ROOT = Path(__file__).parent.parent

raw_sales_asset = Asset("raw_sales_starter")


@dag(
    dag_id="03b_validate_sales_starter",
    start_date=datetime(2026, 5, 1),
    schedule=raw_sales_asset,
    catchup=True,
    tags=["starter"],
)
def validate_sales():

    @task(outlets=Asset("sales_quarantine"))
    def validate_and_insert(**context):
        # TODO 1a: Read the date from the triggering asset event (same pattern as Exercise 3a).
        events = context["triggering_asset_events"].get(raw_sales_asset, [])
        ds = events[0].extra["date"]

        # TODO 1b: Query raw_sales for that date and build a known_isbns set:
        hook = PostgresHook(postgres_conn_id="bookshop_postgres")
        raw_rows = hook.get_records(
            "SELECT isbn, sale_date, quantity, total FROM raw_sales WHERE sale_date = %s", parameters=[ds]
        )
        records = [{"isbn": r[0], "sale_date": str(r[1]), "quantity": r[2], "total": float(r[3])} for r in raw_rows]
        known_isbns = {row[0] for row in hook.get_records("SELECT isbn FROM books")}

        # TODO 1c: Loop through records. A record is bad if quantity <= 0 or isbn not in known_isbns.
        #   Delete existing rows for this date first (idempotency):
        #     hook.run("DELETE FROM daily_sales WHERE sale_date = %s", parameters=[ds])
        #     hook.run("DELETE FROM sales_quarantine WHERE raw->>'sale_date' = %s", parameters=[ds])
        #   valid_rows -> insert into daily_sales (isbn, sale_date, quantity, total)
        #   bad_rows   -> insert into sales_quarantine (raw as JSON string, reason string)
        valid_rows, bad_rows = [], []
        for rec in records:
            if rec["quantity"] <= 0:
                bad_rows.append((json.dumps(rec), f"invalid quantity: {rec['quantity']}"))
            elif rec["isbn"] not in known_isbns:
                bad_rows.append((json.dumps(rec), f"unknown isbn: {rec['isbn']}"))
            else:
                valid_rows.append((rec["isbn"], rec["sale_date"], rec["quantity"], rec["total"]))
        hook.run("DELETE FROM daily_sales WHERE sale_date = %s", parameters=[ds])
        hook.run("DELETE FROM sales_quarantine WHERE raw->>'sale_date' = %s", parameters=[ds])
        hook.insert_rows("daily_sales", valid_rows, target_fields=["isbn", "sale_date", "quantity", "total"])
        if bad_rows:
            hook.insert_rows("sales_quarantine", bad_rows, target_fields=["raw", "reason"])

        # TODO 1d: Return a markdown table string of bad_rows so ApprovalOperator can display it.
        #   Return "No bad records." if bad_rows is empty.
        #pass

        if not bad_rows:
            return "No bad records."
        lines = ["| Reason | Raw Data |", "|--------|----------|"]
        for raw, reason in bad_rows:
            lines.append(f"| {reason} | {raw} |")
        return "\n".join(lines)

    approve = ApprovalOperator(
        task_id="approve_or_reject",
        subject="Quarantined sales records require your review",
        body="""
Date: {{ ds }}

{{ ti.xcom_pull(task_ids='validate_and_insert') }}

Approve to emit the daily_sales asset and trigger the genre report.
Reject to stop this run.
        """,
        outlets=[Asset("daily_sales")],
        response_timeout=timedelta(hours=24),
    )

    validate_and_insert() >> approve


validate_sales()
