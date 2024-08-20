from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import sys
import os

# Import the modules
from solar_project.missing_json import main as find_missing_dates
from solar_project.extraction_json import main as extraction
from solar_project.loading_json import load_json_to_postgres
from solar_project.transforming_json import raw_to_processed

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 17),
    "schedule_interval": "5 23 * * *",
    "email": "myemail@email.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 0,
}

# Define the DAG
with DAG(
    "solar_etl",
    default_args=default_args,
    description="A DAG to ETL data from APsystems",
    catchup=False,
) as dag:

    def failure_callback(context):
        email_task = EmailOperator(
            task_id='send_failure_email',
            to='myemail@email.com',
            subject='Task Failed: {{ task_instance.task_id }}',
            html_content='Task {{ task_instance.task_id }} in DAG {{ dag.dag_id }} failed. Check the logs for details.',
            conn_id='mailtrap', 
        )
        email_task.execute(context)  

    
    task_find_missing_dates = PythonOperator(
        task_id="find_missing_dates",
        python_callable=find_missing_dates,
        on_failure_callback=failure_callback
    )

    task_extraction = PythonOperator(
        task_id="extraction",
        python_callable=extraction,
        on_failure_callback=failure_callback
    )
    
    task_loading_json = PythonOperator(
        task_id = 'load_json_to_postgres',
        python_callable=load_json_to_postgres,
        on_failure_callback=failure_callback
    )
    
    task_transforming = PythonOperator(
        task_id = 'transform_raw_to_processed',
        python_callable=raw_to_processed,
        on_failure_callback=failure_callback
    )
    

    task_find_missing_dates >> task_extraction >> task_loading_json >> task_transforming