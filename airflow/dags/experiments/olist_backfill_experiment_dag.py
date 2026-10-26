from airflow.sdk import DAG, task, get_current_context
from datetime import datetime


with DAG(
    dag_id="olist_backfill_experiment",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
) as dag:

    @task
    def show_processing_interval():
        context = get_current_context()

        start = context["data_interval_start"]
        end = context["data_interval_end"]

        print(f"Processing data from: {start}")
        print(f"Processing data until: {end}")

    show_processing_interval()