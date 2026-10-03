from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# def bronze_summary():
# 	order_count = 99441
	
# 	print(f"Bronze Processed {order_count} orders")
# 	return order_count

# def silver_summary(ti):
# 	order_count = ti.xcom_pull(
# 		task_ids="bronze_summary"
# 	)
# 	print(f"Silver received {order_count} orders from Bronze ")


def bronze_summary(ti):
    order_count = 99441

    ti.xcom_push(
        key="order_count",
        value=order_count
    )

    print(f"Bronze pushed {order_count} orders to XCom")


def silver_summary(ti):
    order_count = ti.xcom_pull(
        task_ids="bronze_summary",
        key="order_count"
    )

    print(f"Silver received {order_count} orders from Bronze")


with DAG(
    dag_id="olist_xcom_experiment",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_summary",
        python_callable=bronze_summary
    )

    silver = PythonOperator(
        task_id="silver_summary",
        python_callable=silver_summary
    )

    bronze >> silver


'''
# Databricks notebook

orders = spark.read.parquet(
    "/Volumes/olist/bronze/orders"
)

order_count = orders.count()

print(f"Bronze processed {order_count} orders")

# Return a SMALL value to Airflow
dbutils.notebook.exit(str(order_count))

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime


def get_notebook_result(ti):

    # Get Databricks run ID stored by the operator
    run_id = ti.xcom_pull(
        task_ids="bronze_databricks",
        key="run_id"
    )

    hook = DatabricksHook(
        databricks_conn_id="databricks_default"
    )

    # Retrieve output from the completed Databricks run
    output = hook.get_run_output(run_id)

    notebook_result = output["notebook_output"]["result"]

    print(f"Databricks returned: {notebook_result}")

    # We can push our own clean XCom value
    ti.xcom_push(
        key="order_count",
        value=int(notebook_result)
    )


with DAG(
    dag_id="databricks_xcom_experiment",
    start_date=datetime(2026, 10, 3),
    schedule=None,
    catchup=False,
) as dag:

    bronze = DatabricksSubmitRunOperator(
        task_id="bronze_databricks",
        databricks_conn_id="databricks_default",

        new_cluster={
            "spark_version": "<runtime-version>",
            "node_type_id": "<node-type>",
            "num_workers": 1,
        },

        notebook_task={
            "notebook_path": "/Workspace/Users/your-email/bronze_notebook"
        },

        do_xcom_push=True
    )

    get_result = PythonOperator(
        task_id="get_bronze_result",
        python_callable=get_notebook_result
    )

    bronze >> get_result
    
    
    
    def silver_task(ti):

    order_count = ti.xcom_pull(
        task_ids="get_bronze_result",
        key="order_count"
    )

    print(f"Bronze processed {order_count} orders")
    
    
    @task
def bronze():
    return 99441


    '''