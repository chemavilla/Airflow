'''
Test DAG for Airflow 

This DAG serves as a test example for Airflow webserver and 
scheduler to verify that the installation is successful.

Version of DAG test.py using the new taskflow API
'''

from datetime import datetime
from airflow.decorators import dag, task
from default_args import common_args

@dag(schedule='@daily',
     start_date=datetime(2024, 2, 1),
     tags=["test"],
     default_args=common_args)
def test_taskflow():
    @task()
    def task1(data): # Equivalent a EmptyOperator
        return None

    @task()
    def task2(data):
        return None

    @task()
    def task3():
        pass

    task1(task2(task3()))

test_taskflow()
