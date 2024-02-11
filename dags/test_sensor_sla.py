"""
This DAG defines a workflow that consists of two sensors, a FileSensor and a PythonSensor,
 and a task to process the file.
The FileSensor waits for a file to be present at the specified filepath, while the PythonSensor
waits for a Python callable to return True. Once both sensors are successful, the process_file
task is triggered to process the file.
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowException
from default_args import common_args

FILE_CSV  = 'test.csv'

def _sla_miss_callback(sla_miss):
    print('sla miss callback ' + sla_miss)

def _failure_callback(context):
    if isinstance(context['exception'], AirflowException):
        print('The task failed context:', context)

@dag(schedule='@daily',
    start_date=datetime(2024, 2, 1),
    tags=["test"],
    sla_miss_callback=_sla_miss_callback,
    default_args=common_args)
def sensor_sla_dag():

    #Wait for the file to be present, reschedule if not present
    file_sensor = FileSensor(task_id='wait_for_file',
                             filepath=FILE_CSV,
                             fs_conn_id='inputs', 
                             poke_interval=120,
                             timeout= 60 * 30,
                             mode='reschedule',
                             on_failure_callback=_failure_callback)

    #Dumb python sensor
    python_sensor = PythonSensor(task_id='wait_process',
                                 python_callable=lambda: False,
                                 mode='poke',
                                 timeout=60 * 30,
                                 poke_interval=120,
                                 on_failure_callback=_failure_callback,
                                 sla=timedelta(minutes=1))

    #If one sensor success, the task is triggered
    @task(trigger_rule='none_failed_min_one_success')
    def process_file(file):
        #raise Exception('Error for testing pourposes with cli airflow dag test')
        print('Process the file')

    [file_sensor, python_sensor] >> process_file(FILE_CSV)

sensor_sla_dag()
