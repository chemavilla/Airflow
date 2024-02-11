"""
DAG that generates random user data using RESTAPI,
then loads it into the Postgres database used by apache docker compose.

"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from default_args import common_args

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

    # Code to create the SQL file with the INSERT statements
    # Pendent to check if is a good solution in a celeryexecuter environment
    file_path = os.environ.get("AIRFLOW_HOME") + "/dags/sql/insert_users_data.sql"
    with open(file_path, "w") as file:
        file.write("INSERT INTO users VALUES\n")
       
        for i in range(0, len(df)):
            if i < len(df)-1:
                file.write(compose_value_sql(df, i) + ',\n')
            else:
                file.write(compose_value_sql(df, i) + ';')

# Connection to postgres defined in Airflow web UI Admin -> Connections
postgress_conn = "airflow_test_conn"

with DAG(dag_id="restapi_2_postgres",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         tags=["test"],
         default_args=common_args) as dag:

    create_table_users = PostgresOperator( # Create table if not exists
        task_id="create_table_users",
        postgres_conn_id=postgress_conn,
        sql="sql/create_table_users.sql"
    )

    # TODO: Refactor to external tool to avoid overload the Airflow server, example Spark
    get_user_api_data = PythonOperator( # Get data from API
        task_id="get_user_api_data",
        python_callable=gen_rnd_user
    )

    insert_users_data = PostgresOperator( # Insert data into table
        task_id="insert_users_data",
        postgres_conn_id=postgress_conn,
        sql="sql/insert_users_data.sql"
    )

    create_table_users >> get_user_api_data >> insert_users_data
