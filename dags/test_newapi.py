'''
Test DAG for Airflow 

This DAG serves as a test example for Airflow webserver and 
scheduler to verify that the installation is successful.

Version of DAG test.py using the new taskflow API
'''

from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id='test_newapi',
    schedule='@daily',
    start_date=datetime(2024, 2, 1),
    catchup=True,
    tags=["test"],
)
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

    # [START main_flow]
    task1(task2(task3()))
    # [END main_flow]


test_taskflow()
