from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="olist_sensor_experiment",
    start_date=datetime(2026, 10, 2),
    schedule=None,
    catchup=False,
) as dag:
        wait_for_file = FileSensor(
        task_id="wait_for_orders_file",
        filepath="/opt/olist/data/incoming/orders_ready.txt",
        poke_interval=10,
        timeout=60,
        mode="poke"
    )
        file_arrived = BashOperator(
        task_id="file_arrived",
        bash_command="echo 'Orders file arrived - pipeline can start!'"
    )

wait_for_file >> file_arrived