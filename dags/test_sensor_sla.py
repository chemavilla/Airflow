from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowException, AirflowFailException

FILE_CSV = 'test.csv'

def _sla_miss_callback():
    print('sla miss callback')

def _failure_callback(context):
    if isinstance(context['exception'], AirflowException):
        print('The task failed context:', context)

@dag(schedule='@daily',
    start_date=datetime(2024, 2, 1), 
    catchup=True, 
    tags=["test"], 
    sla_miss_callback=_sla_miss_callback)
def sensor_sla_dag():
    
    file_sensor = FileSensor(task_id='wait_for_file',
                             filepath=FILE_CSV,
                             poke_interval=120,
                             timeout= 60 * 30,
                             mode='reschedule',
                             on_failure_callback=_failure_callback)

    python_sensor = PythonSensor(task_id='wait_process',
                                 python_callable=lambda: True,
                                 mode='poke',
                                 timeout=60 * 30,
                                 poke_interval=120,
                                 on_failure_callback=_failure_callback)

    @task(trigger_rule='none_failed_min_one_success')
    def process_file(file):
        print('Process the file')

    [file_sensor, python_sensor] >> process_file(FILE_CSV)

sensor_sla_dag()
