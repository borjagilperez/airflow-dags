# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _cleaning():

    print("Cleaning from target DAG")

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(
    "ak8s_dagofdags_worker",
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False) as dag:

    t1 = BashOperator(
        task_id="storing",
        bash_command="sleep 10"
        #bash_command="exit 1"
    )

    t2 = PythonOperator(
        task_id="cleaning",
        python_callable=_cleaning
    )

    t1 >> t2
