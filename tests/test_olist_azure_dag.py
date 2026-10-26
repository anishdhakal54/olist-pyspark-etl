from airflow.models import DagBag
from airflow.dags import olist_azure_pipeline_dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import timedelta,datetime

def test_olist_azure_dag_imports():
    dag_bag = DagBag(
        dag_folder="airflow/dags"
    )

    assert len(dag_bag.import_errors) == 0
    assert "olist_azure_pipeline" in dag_bag.dags

def test_olist_dag_structure():
    dag_bag = DagBag(
        dag_folder="airflow/dags"
    ) 
    dag = dag_bag.dags['olist_azure_pipeline']
    actual = dag.get_task("trigger_databricks_pipeline")

    assert actual.task_id=="trigger_databricks_pipeline"
    assert isinstance(actual,DatabricksRunNowOperator)
    assert actual.databricks_conn_id == "databricks_default"

    assert actual.retries == 2
    assert actual.retry_delay == timedelta(minutes = 2)