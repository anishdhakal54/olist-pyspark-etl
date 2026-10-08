from airflow.models import DagBag


def test_olist_dag_imports():
    dag_bag = DagBag(
        dag_folder="airflow/dags"
    )

    assert len(dag_bag.import_errors) == 0
    assert "olist_local_pipeline" in dag_bag.dags