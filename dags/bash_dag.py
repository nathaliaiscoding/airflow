from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

with DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2022, 1, 1), catchup=False) as dag:
    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command="echo 'command executed from BashOperator'"
    )

    dummy_task >> bash_task
