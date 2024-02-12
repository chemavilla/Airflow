from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from default_args import common_args

with DAG(dag_id='jinja_get_data_GCP',
         schedule='@daily',
         start_date=datetime( 2024 , 1 ,1),
         catchup=False,
         tags=["GCP", "jinja"],
         default_args=common_args) as dag:
    
    @task()
    def extract(data):
        return data

    @task()
    def transform(data):
        return data

    @task()
    def load(data):
        print("Data input ", data)

    load(transform(extract("gs://chemavilla/Price_table.csv")))