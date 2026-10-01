from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.sdk import get_current_context

default_args= {
    "retries":2,
    "retry_delay":timedelta(seconds=15)
}
def start_pipeline():
    print("Starting Olist pipeline")

def run_bronze():
    print("Running bronze layer")

def run_silver():
    # context = get_current_context()
    # task_instance = context['task_instance']
    # print("Try number:", task_instance.try_number)
    # if task_instance.try_number == 1:
    #     raise Exception("Temporary Silver failure!")

    print("Running silver layer")

def run_gold():
    print("Running gold layer")

def failure_alert():
    print("Alert: Olist pipeline task failed")

with DAG(
    dag_id= "olist_pipeline_experiment",
    default_args=default_args,
    start_date=datetime(2026,9,25),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False
):
    start_task= PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline
    )
    bronze_task = DatabricksSubmitRunOperator(
    task_id="run_bronze",
    databricks_conn_id="databricks_default",
    json={
        "tasks": [
            {
                "task_key": "bronze",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/01_bronze_ingestion",
                    "base_parameters": {
                        "storage_account": "stolistdataanish",
                        "container": "olist"
                    }
                }
            }
        ]
    },
)

##if actual cluster is running instead of serverless we use below code
#   notebook_task={
#             "notebook_path": (
#                 "/Workspace/Users/anish.dhakal54@gmail.com/"
#                 "olist-pyspark-etl/databricks/03_gold"
#             ),
#             "base_parameters": {
#                 "storage_account": "stolistdataanish",
#                 "container": "olist",
#             },
#         },
#     )

    
    silver_task = DatabricksSubmitRunOperator(
    task_id="run_silver",
    databricks_conn_id="databricks_default",
    json={
        "tasks": [
            {
                "task_key": "silver",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/02_silver_processing",
                    "base_parameters": {
                        "storage_account": "stolistdataanish",
                        "container": "olist"
                    }
                }
            }
        ]
    }
)
    gold_task = DatabricksSubmitRunOperator(
    task_id="run_gold",
    databricks_conn_id="databricks_default",
    json={
        "tasks": [
            {
                "task_key": "gold",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/03_gold",
                    "base_parameters": {
                        "storage_account": "stolistdataanish",
                        "container": "olist"
                    }
                }
            }
        ]
    }
)

    failure_alert_task= PythonOperator(
        task_id="failure_alert",
        python_callable=failure_alert,
        trigger_rule = "one_failed"
    )

    start_task>>bronze_task>>silver_task>>gold_task
    bronze_task >> failure_alert_task
    silver_task >> failure_alert_task
    gold_task >> failure_alert_task

