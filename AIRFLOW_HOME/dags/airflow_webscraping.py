import sys

sys.path.insert(0,"C:\AIRFLOW_HOME\scripts")
from scripts.user_agent_list import user_agent_list

from scripts.extract_data_super_pharm import extract_data
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import sys
import os


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 1, 1),
    'concurrency': 1,
    'retries': 1
}

with DAG('my_web_scraper',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/10 * * * *'
         ) as dag:
    opr_hello = BashOperator(task_id='say_Hi',
                             bash_command='echo "Starting"')

    opr_extract_data = PythonOperator(task_id='exctract_data',
                                 python_callable=extract_data)
    opr_data_done = BashOperator(task_id='data_extracted',
                             bash_command='Extracting data is Done')

opr_hello  >> opr_extract_data  >> opr_data_done 



