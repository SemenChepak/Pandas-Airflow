import os
import sys
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pand import db_in
from pand import df_creator
from pand.files_worker import file_worker

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

default_args = {
    'owner': 'Pandas',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
        'Pandas',
        default_args=default_args,
        description='data extraction',
        schedule_interval=timedelta(minutes=30),
        start_date=datetime(2021, 11, 17),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='extract_data',
        python_callable=file_worker.extract_from_site,
        dag=dag
    )
    t2 = PythonOperator(
        task_id='get_files_names',
        python_callable=file_worker.files,
        dag=dag
    )

    t3 = PythonOperator(
        task_id='create_dfs',
        python_callable=df_creator.collect_df,
        op_kwargs={'files': t2.output},
        dag=dag
    )

    t4 = PythonOperator(
        task_id='dell_dir_with_files',
        python_callable=file_worker.delete_directory,
        dag=dag
    )

    t5 = PythonOperator(
        task_id='insert_into_db',
        python_callable=db_in.ins_data_to_db,
        op_kwargs={'df': t3.output},
        dag=dag
    )

    t1 >> t2 >> t3 >> t4
    t3 >> t5
