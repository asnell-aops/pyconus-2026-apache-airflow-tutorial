import csv  # noqa: F401
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


REPO_ROOT = Path(__file__).parent.parent


@dag(
    dag_id="01_ingest_books_starter",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["starter"],
)
def ingest_books():

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="bookshop_postgres",
        sql=(REPO_ROOT / "sql" / "schema.sql").read_text(),
    )

    @task
    def load_books():
        hook = PostgresHook(postgres_conn_id="bookshop_postgres")

        # TODO 1: Read data/books.csv and build a list of row tuples.
        # Use csv.reader, skip the header row with next(reader).
        # Each row should be a tuple: (isbn, title, author, genre, price)
        rows = []
        with open(REPO_ROOT / "data" / "books.csv") as f:
            reader = csv.reader(f)
            next(reader)  # skip the header
            rows = [tuple(row) for row in reader]

        # TODO 2: Insert rows into the books table using hook.insert_rows.
        # Use replace=True and replace_index="isbn" to make it safe to rerun.
        # target_fields=["isbn", "title", "author", "genre", "price"]
        hook.insert_rows(
            table="books",
            rows=rows,
            target_fields=["isbn", "title", "author", "genre", "price"],
            replace_index="isbn",
            replace=True,
        )
        return len(rows)

    reconcile = SQLExecuteQueryOperator(
        task_id="reconcile",
        conn_id="bookshop_postgres",
        sql="SELECT COUNT(*) FROM books",
    )

    create_tables >> load_books() >> reconcile


ingest_books()
