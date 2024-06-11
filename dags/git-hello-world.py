from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# importing time module
import time


default_args = {
    'owner': 'PoornaGanesan',
    'start_date': datetime(2024, 3, 1, 12, 0, 0)
}


def compass_hello_world_loop():
    for palabra in ['Hello GIT Cool! Test', 'world']:
        print(palabra)
    print("Before the sleep statement")
    time.sleep(120)
    print("After the sleep statement")

with DAG(
    dag_id='git_hello_world',
    default_args = default_args,
        schedule_interval=timedelta(hours=6),
    max_active_runs=1,
    catchup=False
  #  schedule_interval=timedelta(minutes=15)
) as dag:

    test_start = DummyOperator(task_id='test_start')

    test_python = PythonOperator(task_id='test_python', python_callable=compass_hello_world_loop)

    test_bash =  BashOperator(task_id='test_bash', bash_command='echo Compass Hello World!')

test_start >> test_python >> test_bash
