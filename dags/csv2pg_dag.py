'''
WORK IN PROGRESS

DAG that waits for a local csv file to be created, then loads it into a Postgres database.


from airflow import DAG
from airflow.operators.python               import PythonOperator
from airflow.providers.postgres.hooks       import PostgresHook
from airflow.providers.postgres.operators   import PostgresOperator

from datetime import datetime

def create_table():
    # Create a PostgresHook instance
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    # Execute a SQL query to create a table
    query = """
    CREATE TABLE IF NOT EXISTS my_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        age INT
    )
    """
    hook.run(query)

def my_task():
    # Create a PostgresHook instance
    hook = PostgresHook(postgres_conn_id='my_postgres_conn')

    # Execute a SQL query
    query = "SELECT * FROM my_table"
    result = hook.get_records(query)
    print(result)

with DAG('0_my_dag', schedule_interval='@daily', start_date=datetime(2022, 1, 1)) as dag:
    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='my_postgres_conn',
        sql=create_table
    )

    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task
    )

    create_table_task >> task

'''