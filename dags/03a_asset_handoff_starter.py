from datetime import datetime
from airflow.sdk import dag, task, Asset


# Asset objects even if created in different DAG files will be the same asset if they have the same name.
# This allows us to share assets across DAGs without having to import them from a common module.
raw_files_assets = Asset("raw_files_assets")


# --- DAG 1: Producer ---
@dag(
    dag_id="03a_producer_starter",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    tags=["starter"],
)
def asset_producer():
    @task(outlets=[raw_files_assets])
    def emit_asset(ds=None, **context):
        print(f"Emitting raw_files_assets asset for {ds}")
        # TODO: Attach {"date": ds, "count": 42} to the asset event so the consumer can read it.
        # Hint: context["outlet_events"][raw_files_assets].extra = {...}

    emit_asset()


asset_producer()


# --- DAG 2: Consumer ---
@dag(
    dag_id="03a_consumer_starter",
    start_date=datetime(2026, 5, 1),
    schedule=raw_files_assets,
    catchup=False,
    tags=["starter"],
)
def asset_consumer():
    @task
    def print_event(**context):
        # TODO: Read triggering_asset_events for raw_files_assets and print event.extra for each event.
        # Hint: context["triggering_asset_events"].get(raw_files_assets, [])
        pass

    print_event()


asset_consumer()
