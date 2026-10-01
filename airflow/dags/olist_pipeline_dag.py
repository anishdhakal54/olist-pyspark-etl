from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow import DAG
from datetime import datetime,timedelta


default_args = {
    "retries":2,
    "retry_delay": timedelta(minutes =2)
}

def run_bronze():
    print("Bronze layer is running")

def run_silver():
    print("Silver layer is running")

with DAG(
    dag_id= "olist_pipeline",
    default_args=default_args,
    start_date=datetime(2026,9,28),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
):
    bronze_task = DatabricksSubmitRunOperator(
    task_id="run_bronze",
    databricks_conn_id ="databricks_default",
    json = {
        "tasks":[
            {
            "task_key":"bronze",
            "notebook_task":{
                "notebook_path":"/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/01_bronze_ingestion",
                "base_parameters":{
                    "storage_account":"stolistdataanish",
                    "container":"olist"
                        }
                    }
              }

            ]
        },
    )
    silver_task = DatabricksSubmitRunOperator(
        task_id="run_silver",
        databricks_conn_id ="databricks_default",
        json = {
            "tasks":[
                {
                "task_key":"silver",
                "notebook_task":{
                    "notebook_path":"/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/02_silver_processing",
                    "base_parameters":{
                        "storage_account":"stolistdataanish",
                        "container":"olist"
                            }
                        }
                  }
    
                ]
            },
        )

    gold_task = DatabricksSubmitRunOperator(
            task_id="run_gold",
            databricks_conn_id ="databricks_default",
            json = {
                "tasks":[
                    {
                    "task_key":"gold",
                    "notebook_task":{
                        "notebook_path":"/Workspace/Users/anish.dhakal54@gmail.com/olist-pyspark-etl/databricks/03_gold",
                        "base_parameters":{
                            "storage_account":"stolistdataanish",
                            "container":"olist"
                                }
                            }
                      }
        
                    ]
                },
            )

    bronze_task>>silver_task>>gold_task