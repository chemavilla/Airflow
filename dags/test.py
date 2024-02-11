'''
Test DAG for Airflow 

This DAG serves as a test example for Airflow webserver and 
scheduler to verify that the installation is successful.

Old Airflow api is used
'''

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from default_args import common_args

# Define the DAG, 0 for sorting purposes 
with DAG(dag_id='test',
         schedule='@daily',
         start_date=datetime(2024, 2, 1),
         tags=["test"],
         default_args=common_args) as dag:

    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')

# Define the task dependencies
task1 >> task2 >> task3
