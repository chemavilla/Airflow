'''
Test Operators DAG for Airflow 

This DAG serves as a test example for Airflow Operators.

Old apache api is used
'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator  
from datetime import datetime
from default_args import common_args


with DAG(dag_id='test_operators',
         schedule='@daily',
         start_date=datetime(2024, 2, 1),
         tags=["test"],
         default_args=common_args) as dag:

    def _print_exec_date(execution_date, **context):
        print('Execution date = ', execution_date)

    task1 = BashOperator(task_id='task1', bash_command='echo "Hello, world"')
    task2 = BashOperator(task_id='task2', bash_command='echo "Hello, Airflow"')
    
    print_exec_date = PythonOperator(task_id="print_exec_date", python_callable=_print_exec_date
    )

# Define the dependencies between the tasks
task1 >> task2
task1 >> print_exec_date
