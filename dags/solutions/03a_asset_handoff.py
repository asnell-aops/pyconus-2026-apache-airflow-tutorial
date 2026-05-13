from datetime import datetime
from airflow.sdk import dag, task, Asset


# Asset objects even if created in different DAG files will be the same asset if they have the same name.
# This allows us to share assets across DAGs without having to import them from a common module.
raw_files_assets = Asset("raw_files_assets")


# --- DAG 1: Producer ---
@dag(
    dag_id="03a_producer",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    tags=["solution"],
)
def asset_producer():
    @task(outlets=[raw_files_assets])
    def emit_asset(ds=None, **context):
        print(f"Emitting raw_files_assets asset for {ds}")
        context["outlet_events"][raw_files_assets].extra = {"date": ds, "count": 42}

    emit_asset()


asset_producer()


# --- DAG 2: Consumer ---
@dag(
    dag_id="03a_consumer",
    start_date=datetime(2026, 5, 1),
    schedule=raw_files_assets,
    catchup=False,
    tags=["solution"],
)
def asset_consumer():
    @task
    def print_event(**context):
        events = context["triggering_asset_events"].get(raw_files_assets, [])
        for event in events:
            print(f"raw_files_assets fired | extra: {event.extra}")

    print_event()


asset_consumer()
