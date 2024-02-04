'''
Test Operators DAG for Airflow 

This DAG serves as a test example for Airflow Operators.

Old apache api is used
'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator  

from datetime import datetime

# Define the DAG arguments

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
}

def print_exec_date(execution_date, **context):
    print('Execution date = ', execution_date)

with DAG(dag_id='test_operators',
         default_args=default_args,
         start_date=datetime(2024, 2, 1),
         catchup=True,
         tags=["test"],
         schedule='@daily') as dag:
    
    task1 = BashOperator(task_id='task1', bash_command='echo "Hello, world"')
    task2 = BashOperator(task_id='task2', bash_command='echo "Hello, Airflow"')
    
    print_exec_date = PythonOperator(task_id="print_exec_date", python_callable=print_exec_date
    )

# Define the dependencies between the tasks
task1 >> task2
task1 >> print_exec_date
