from airflow.sdk import DAG, task
from datetime import datetime
import time


with DAG(
    dag_id="olist_pool_experiment",
    start_date=datetime(2026, 10, 5),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def get_datasets():
        return [
            "orders",
            "customers",
            "products",
            "sellers"
        ]

    @task(pool="olist_pool")
    def process_dataset(dataset):
        print(f"Starting {dataset}")

        time.sleep(30)

        print(f"Finished {dataset}")

    datasets = get_datasets()

    process_dataset.expand(dataset=datasets)


    '''
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="olist_databricks_pool_experiment",
    default_args=default_args,
    start_date=datetime(2026, 10, 5),
    schedule=None,
    catchup=False,
) as dag:

    bronze = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        pool="databricks_pool",

        existing_cluster_id="<YOUR_CLUSTER_ID>",

        notebook_task={
            "notebook_path": (
                "/Workspace/Users/<YOUR_DATABRICKS_USER>//"
                "olist-pyspark-etl/databricks/01_bronze_ingestion"
            ),
            "base_parameters": {
                "storage_account": "<YOUR_STORAGE_ACCOUNT>",
                "container": "<YOUR_CONTAINER>",
            },
        },
    )

    silver = DatabricksSubmitRunOperator(
        task_id="silver_processing",
        databricks_conn_id="databricks_default",
        pool="databricks_pool",

        existing_cluster_id="<YOUR_CLUSTER_ID>",

        notebook_task={
            "notebook_path": (
                "/Workspace/Users/<YOUR_DATABRICKS_USER>//"
                "olist-pyspark-etl/databricks/02_silver_processing"
            ),
            "base_parameters": {
                "storage_account": "<YOUR_STORAGE_ACCOUNT>",
                "container": "<YOUR_CONTAINER>",
            },
        },
    )

    gold = DatabricksSubmitRunOperator(
        task_id="gold_curation",
        databricks_conn_id="databricks_default",
        pool="databricks_pool",

        existing_cluster_id="<YOUR_CLUSTER_ID>",

        notebook_task={
            "notebook_path": (
                "/Workspace/Users/<YOUR_DATABRICKS_USER>//"
                "olist-pyspark-etl/databricks/03_gold"
            ),
            "base_parameters": {
                "storage_account": "<YOUR_STORAGE_ACCOUNT>",
                "container": "<YOUR_CONTAINER>",
            },
        },
    )

    bronze >> silver >> gold
    
    '''