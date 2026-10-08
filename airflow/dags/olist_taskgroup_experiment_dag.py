from airflow.sdk import DAG, task, TaskGroup
from datetime import datetime


with DAG(
    dag_id="olist_taskgroup_experiment",
    start_date=datetime(2026, 10, 4),
    schedule=None,
    catchup=False,
) as dag:

    with TaskGroup(group_id="validation") as validation_group:

        @task
        def validate_orders():
            print("Validating orders")

        @task
        def validate_customers():
            print("Validating customers")

        orders = validate_orders()
        customers = validate_customers()


    with TaskGroup(group_id="processing") as processing_group:

        @task
        def process_orders():
            print("Processing orders")

        @task
        def process_customers():
            print("Processing customers")

        processed_orders = process_orders()
        processed_customers = process_customers()

    validation_group >> processing_group
    