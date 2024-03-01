import logging

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from pendulum import datetime

with DAG(
    dag_id="dynamic_task_group",
    description="Task Group on top of Dynamic Mapped Tasks",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval=None,
) as dag:

    @task
    def create_instructions(**kwargs):
        # kwargs if you need an input params
        # input = kwargs.get("params")
        # try:
        #     ingest_cfg = input["ingest"]
        return [
            {"pipeline_name": 1, "parameters": 2},
            {"pipeline_name": 3, "parameters": 4},
        ]

    @task
    def print_values(pipeline_name, parameters):
        logging.info(f"pipeline_name: {pipeline_name}")
        logging.info(f"parameters: {parameters}")
        if pipeline_name == 3:
            raise AirflowException("Test error status")

    @task
    def print_values_again(pipeline_name, parameters):
        logging.info(f"pipeline_name: {pipeline_name}")
        logging.info(f"parameters: {parameters}")

    @task_group
    def my_group(pipeline_name, parameters):
        one = print_values(pipeline_name, parameters)
        two = print_values_again(pipeline_name, parameters)

        one >> two

    my_group.expand_kwargs(create_instructions())
