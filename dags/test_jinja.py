from airflow.decorators import dag, task
from datetime import datetime, timedelta
from default_args import common_args

#TODO: Work in progress, tutorial https://marclamberti.com/blog/templates-macros-apache-airflow/ 
@dag(schedule='@daily',
    start_date=datetime(2024, 2, 1),
    tags=["test"],
    default_args=common_args)
def jinja_macro():
    @task()
    def task1():
        pass


jinja_macro()
