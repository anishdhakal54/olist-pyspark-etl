from airflow.models import DagBag


def test_olist_dag_imports():
    dag_bag = DagBag(
        dag_folder="airflow/dags"
    )

    assert len(dag_bag.import_errors) == 0
    assert "olist_local_pipeline" in dag_bag.dags


def test_olist_dag_structure():
    dag_bag = DagBag(
        dag_folder="airflow/dags"
    ) 
    dag = dag_bag.dags['olist_local_pipeline']
    bronze = dag.get_task("bronze_ingestion")

    silver = dag.get_task("silver_processing")
    gold = dag.get_task("gold_curation")

    assert bronze.downstream_task_ids =={"silver_processing"}
    assert silver.downstream_task_ids =={"gold_curation"}
    assert gold.downstream_task_ids ==set()

    assert bronze.retries == 2
    assert silver.retries == 2
    assert gold.retries == 2