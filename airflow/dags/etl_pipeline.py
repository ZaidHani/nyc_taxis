from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append('dags/Modules/')

from Modules.ETL import etl
from Modules.Pipeline import download_batchly

default_args = {
    'owner': 'Zaid',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='etl_pipeline',
    default_args= default_args,
    description='this dag is for the etl pipeline, it will download the data from the website monthly and do the etl process on the files',
    start_date=datetime(2023, 12, 9),
    schedule_interval='@monthly',

) as dag:
    t1 = PythonOperator(
        task_id = 'download_the_data',
        python_callable = download_batchly,
        dag = dag
    )
    t2a = PythonOperator(
        task_id = 'etl_yellow',
        python_callable= etl,
        op_args={'type': 'yellow'}
    )
    t2b = PythonOperator(
        task_id = 'etl_green',
        python_callable= etl,
        op_args={'type': 'green'}
    )
    t2c = PythonOperator(
        task_id = 'etl_fhv',
        python_callable= etl,
        op_args={'type': 'fhv'}
    )
    t2d = PythonOperator(
        task_id = 'etl_fhvhv',
        python_callable= etl,
        op_args={'type': 'fhvhv'}
    )

t1 >> t2a
t1 >> t2b
t1 >> t2c
t1 >> t2d
