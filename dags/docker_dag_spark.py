from multiprocessing import dummy
from sys import api_version
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os.path

default_args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def checkIfRepoAlreadyCloned():
    if os.path.exists('/home/airflow/simple-app/.git'):
        return dummy
    return 'git_clone'


with DAG('docker_dag', default_args=default_args, schedule_interval='5 * * * *', catchup=False) as dag:
    t_git_clone = BashOperator(
        task_id='git_clone',
        bash_command='git clone https://github.com/marclamberti/simple-app.git /home/airflow/simple-app'
    )

    t_git_pull = BashOperator(
        task_id='git_pull',
        bash_command='cd /home/airflow/simple-app && git pull',
        trigger_rule='one_success'
    )

    t_check_repo = BranchPythonOperator(
        task_id='is_repo_exists',
        python_callable=checkIfRepoAlreadyCloned
    )

    t_dummy = DummyOperator(
        task_id='dummy_task'
    )

    t_check_repo >> t_git_clone
    t_check_repo >> t_dummy >> t_git_pull
    t_git_clone >> t_git_pull

    t_docker = DockerOperator(
        task_id='docker_command',
        image='bde2020/spark-master:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/airflow/simple-app:/simple-app'],
        command='/spark/bin/spark-submit --master local[*] /simple-app/SimpleApp.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t_git_pull >> t_docker
