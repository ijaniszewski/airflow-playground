import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


def training():
    return random.randint(0, 10)


def _choose_best_model(ti):
    results = ti.xcom_pull(task_ids=["training_A", "training_B", "training_C"])
    if max(results) > 8:
        return "accurate"
    return "inaccurate"


with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    training_model_A = PythonOperator(task_id="training_A", python_callable=training)
    training_model_B = PythonOperator(task_id="training_B", python_callable=training)
    training_model_C = PythonOperator(task_id="training_C", python_callable=training)

    choose_best_model = BranchPythonOperator(
        task_id="choose_best", python_callable=_choose_best_model
    )

    accurate = BashOperator(task_id="accurate", bash_command="echo 'accurate'")

    inaccurate = BashOperator(task_id="inaccurate", bash_command="echo 'inaccurate'")

    (
        [training_model_A, training_model_B, training_model_C]
        >> choose_best_model
        >> [accurate, inaccurate]
    )
