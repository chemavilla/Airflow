from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'test_dag',
    description='A simple test DAG',
    start_date=datetime(2022, 1, 1),
    schedule_interval='@daily',
)

# Define the tasks
task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)

# Define the task dependencies
task1 >> task2 >> task3
