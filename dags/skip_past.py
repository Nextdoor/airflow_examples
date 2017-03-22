from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

import util


def extract_from_source_db(**kwargs):
    logging.info("Extracting from source db ...")


def transform_data(**kwargs):
    logging.info("Transforming data ...")


def load_target_db(**kwargs):
    logging.info("Loading to target db ...")


# Create DAG
dag = DAG('skip_past',
          start_date=datetime(2017, 1, 1),
          schedule_interval=timedelta(days=1)
          )


# Skip unnecessary executions
doc = """
Skip the subsequent tasks if
    a) the execution_date is in past
    b) there multiple dag runs are currently active
"""
start_task = ShortCircuitOperator(
    task_id='skip_check',
    python_callable=util.is_latest_active_dagrun,
    provide_context=True,
    depends_on_past=True,
    dag=dag
)
start_task.doc = doc


# Extract
doc = """Extract from source database"""
extract_task = PythonOperator(
    task_id='extract_from_db',
    python_callable=extract_from_source_db,
    provide_context=True
)
extract_task.doc = doc
start_task >> extract_task


# Transform
doc = """Transform data"""
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True
)
transform_task.doc = doc
extract_task >> transform_task


# Load
doc = """Load target database"""
load_task = PythonOperator(
    task_id='load_target_db',
    python_callable=load_target_db,
    provide_context=True
)
load_task.doc = doc
transform_task >> load_task
