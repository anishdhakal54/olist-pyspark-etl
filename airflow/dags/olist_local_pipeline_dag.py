from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
}


with DAG(
    dag_id="olist_local_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="cd /opt/olist && python -m pipeline.bronze"
    )

    silver = BashOperator(
        task_id="silver_processing",
        bash_command="cd /opt/olist && python -m pipeline.silver"
    )

    gold = BashOperator(
        task_id="gold_curation",
        bash_command="cd /opt/olist && python -m pipeline.gold"
    )

    bronze >> silver >> gold