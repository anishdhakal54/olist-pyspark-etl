from airflow.sdk import DAG,task
from datetime import datetime

with DAG(
	dag_id="olist_dynamic_mapping_expiremnet",
	start_date = datetime(2026,10,3),
	schedule = None,
	catchup=False,
)as dag:
	@task
	def get_datasets():
		return [
        	"orders",
        	"customers",
        	"products",
        	"sellers"
    ]

	@task
	def process_dataset(dataset):
		print(f"Processing {dataset}")
		if dataset == "products":
			raise ValueError("Products processing failed")

		print(f"{dataset} processed successfully")





	@task(trigger_rule="all_done")
	def finish_processing():
		print("All datasets finished")

	datasets = get_datasets()

	mapped_tasks = process_dataset.expand(dataset=datasets)

	finish = finish_processing()

	mapped_tasks >> finish
