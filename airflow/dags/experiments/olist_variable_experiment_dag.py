from airflow.sdk import DAG, task, Variable
from datetime import datetime

with DAG(
    dag_id="olist_variable_experiment",
    schedule=None,
    start_date=datetime(2026,10,4),
    catchup=False,
) as dag:

    @task
    def show_enviroment():
        enviroment = Variable.get('environment')

        print(f"Running in {enviroment} environment")

    show_enviroment()