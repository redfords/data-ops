from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# define the default dag arguments
default_args = {
	'owner': 'joana',
	'depends_on_past': False,
	'email': ['joanapiovaroli@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes = 1)
	}

# define the dag, start date and frequency
dag = DAG(
	dag_id = 'stock_dag',
	default_args = default_args,
	start_date = datetime(2021,7,6),
	schedule_interval = timedelta(minutes = 1440)
	)
