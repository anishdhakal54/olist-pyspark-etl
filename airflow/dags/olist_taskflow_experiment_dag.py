from airflow.sdk import DAG, task
from datetime import datetime


with DAG(
    dag_id="olist_taskflow_experiment",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
) as dag:

    @task(multiple_outputs = True)
    def bronze_summary():
        return {
            "row_count": 99441,
            "path": "data/bronze/orders",
            "status": "success"
        }

    @task
    def silver_summary(bronze_info):
        print(f"Rows: {bronze_info['row_count']}")
        print(f"Path: {bronze_info['path']}")
        print(f"Status: {bronze_info['status']}")

    bronze_info = bronze_summary()

    silver_summary(bronze_info)


'''

    
    bronze = DatabricksSubmitRunOperator(
    task_id="bronze_databricks",
    databricks_conn_id="databricks_default",

    notebook_task={
        "notebook_path": "/Workspace/Users/.../01_bronze_ingestion"
    },

    existing_cluster_id="YOUR_CLUSTER_ID",

    do_xcom_push=True
)
@task
def show_run_id(ti=None):

    run_id = ti.xcom_pull(
        task_ids="bronze_databricks",
        key="run_id"
    )

    print(f"Databricks run ID: {run_id}")
    
	'''



'''
orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)



import json

result = {
    "row_count": orders.count(),
    "path": bronze_path,
    "status": "success"
}

dbutils.notebook.exit(json.dumps(result))




'''