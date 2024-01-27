"""
DAG that generates random user data using RESTAPI,
then loads it into a Postgres database.

"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

def gen_rnd_user():
    """
    Generates random user data using the randomuser.me API.
    Create a file with the SQL code to insert the data into the database.
    """

    import pandas as pd # local import for faster DAG load
    import requests
    import os

    # Code to request random user data and store it in a pandas dataframe
    url_rnd_usr = "https://randomuser.me/api/?nat=es&format=json&results=50"
    req = requests.get(url_rnd_usr)
    assert req.status_code == 200, "ERROR: Cannot generate random user data"
    df = pd.json_normalize(req.json()["results"])

    def compose_value_sql(aux_df, aux_idx):
        """Generate a row of data for insertion into the users table"""

        return "('" + aux_df["login.uuid"][aux_idx] + "','" + \
                     aux_df["name.first"][aux_idx] + " " + aux_df["name.last"][aux_idx] + "','" + \
                     aux_df["id.value"][aux_idx] + "','" + \
                     aux_df["location.city"][aux_idx] + "','" + \
                     aux_df["location.state"][aux_idx] + "','" + \
                     aux_df["gender"][aux_idx] + "','" + \
                     aux_df["email"][aux_idx] + "','" + \
                     aux_df["phone"][aux_idx] + "')"

    file_path = os.environ.get("AIRFLOW_HOME") + "/dags/sql/insert_users_data.sql"
    with open(file_path, "w") as file:
        # Write your new content here
        file.write("INSERT INTO users VALUES\n")
       
        for i in range(0, len(df)):
            file.write(compose_value_sql(df, i) + ',\n')  # Compose rows of data for insertion
        file.write(compose_value_sql(df, len(df)-1) + ';')

# Connection to postgres defined in Airflow web UI Admin -> Connections
postgress_conn = "airflow_test_conn"

with DAG(dag_id="0_postgres_dag",
         start_date=datetime(2024, 1, 26),
         schedule="@daily") as dag:

    create_table_users = PostgresOperator(
        task_id="create_table_users",
        postgres_conn_id=postgress_conn,
        sql="sql/create_table_users.sql"
    )

    get_user_api_data = PythonOperator(
        task_id="get_user_api_data",
        python_callable=gen_rnd_user
    )

    insert_users_data = PostgresOperator(
        task_id="insert_users_data",
        postgres_conn_id=postgress_conn,
        sql="sql/insert_users_data.sql"
    )

    create_table_users >> get_user_api_data >> insert_users_data
