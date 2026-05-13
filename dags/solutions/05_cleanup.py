import csv
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


REPO_ROOT = Path(__file__).parent.parent.parent


@dag(
    dag_id="05_cleanup_tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "starter"],
)
def cleanup_tables():

    drop_tables = SQLExecuteQueryOperator(
        task_id="drop_tables",
        conn_id="bookshop_postgres",
        sql= (REPO_ROOT / "sql" / "reset.sql").read_text()
    )

    drop_tables

cleanup_tables()