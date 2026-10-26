from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow import DAG
from datetime import timedelta,datetime

default_args= {
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}


with DAG(
    dag_id= 'olist_azure_pipeline',
    default_args=default_args,
    description="Trigger the Olist ETL pipeline on Azure Databricks",
    start_date=datetime(2026,10,26),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True
    
) as dag:
    trigger_databricks_pipeline = DatabricksRunNowOperator(
        task_id="trigger_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=43510633667721
    )