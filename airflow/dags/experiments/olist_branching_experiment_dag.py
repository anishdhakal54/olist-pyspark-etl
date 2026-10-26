from airflow.sdk import DAG,task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


with DAG(
	dag_id= "olist_branch_experiment",
	start_date = datetime(2026,10,3),
	schedule= None,
	catchup=False,
) as dag:
	
	@task
	def calculate_data_quality():
		invalid_rows = 23
		return invalid_rows


	
	@task.branch
	def check_quality(invalid_rows):
		if invalid_rows ==0:
			return "run_gold"
		else:
			return "handle_bad_data"

	run_gold = EmptyOperator(
		task_id ="run_gold"
	)
	handle_bad_data = EmptyOperator(
		task_id = "handle_bad_data"
	)
	finish_pipeline = EmptyOperator(
		task_id = "finish_pipeline",
		trigger_rule = "none_failed_min_one_success"
	)

	invalid_rows = calculate_data_quality()
	quality_check = check_quality(invalid_rows)
	quality_check >>[run_gold,handle_bad_data]
	[run_gold,handle_bad_data]>>finish_pipeline
	