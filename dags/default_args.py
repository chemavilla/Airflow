from datetime import datetime, timedelta

common_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': True,
    'sla': timedelta(minutes=30),
    'email': ['chemavilla@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}