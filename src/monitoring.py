import logging


def alert_on_failure(context):
    ti = context["ti"]
    task_id = ti.task_id
    dag_id = ti.dag_id
    exception = context.get("exception")

    message = f"{dag_id} got an error at {task_id} with {exception}"

    logging.error(f"🚨 CUSTOM OLIST ALERT 🚨 {message}")