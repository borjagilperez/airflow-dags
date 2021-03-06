# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def _downloading():

    print("downloading")

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(
    "local_dagofdags_driver",
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False) as dag:

    t1 = PythonOperator(
        task_id="downloading",
        python_callable=_downloading
    )

    t2 = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="dagofdags_worker",
        #execution_date='{{ ds }}',
        execution_date='{{ ts }}',
        reset_dag_run=True,
        wait_for_completion=True,
        #poke_interval=60
        poke_interval=5
    )

    t1 >> t2
