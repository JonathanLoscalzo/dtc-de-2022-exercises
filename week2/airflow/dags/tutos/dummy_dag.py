from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago
import logging
import requests


dag = DAG(
    "dummy_dag",
    tags=["dummy"],
    start_date=datetime(2022,1,1),
    schedule_interval=timedelta(days=1)
)

@task
def dummy_task():
    logging.info("SANATA 1")

    return 
    
    url = 'https://www.facebook.com/favicon.ico'
    r = requests.get(url, allow_redirects=True)

    open('facebook.ico', 'wb').write(r.content)

@task
def dummy_task2():
    logging.info("SANATA 2")


with dag: 
    sanata1 = dummy_task()

    sanata2 = dummy_task2()

sanata1 >> sanata2