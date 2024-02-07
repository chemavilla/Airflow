"""
This DAG test XCOM operators comunications by simulating the training 
of two ML models, and chooses the best model based on their accuracy.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(dag_id='xcoms',
    start_date=datetime(2024, 2, 1),
    catchup=True,
    tags=["test"],
)
def choose_model_dag():
    def training_model_A(ti):
        import random 
        ti.xcom_push(key='accuracy', value=random.random())

    def choose_model(ti):
        accuracies = ti.xcom_pull(key='accuracy', task_ids=['train_model_A', 'train_model_B'])
        model, accu = ('A', 0) if accuracies[0] > accuracies[1] else ('B', 1)
        
        print(f"Deploy Model {model} accu = {accuracies[accu]}")
        ti.xcom_push(key='model', value='A')
     
    download_data = BashOperator(
        task_id='download_data',
        bash_command='sleep 3',
        do_xcom_push=False)

    train_model_A = PythonOperator(
            task_id='train_model_A',
            python_callable=training_model_A,
        )
    
    train_model_B = PythonOperator(
            task_id='train_model_B',
            python_callable=training_model_A,
        )

    choose_model = PythonOperator(
            task_id='choose_model',
            python_callable=choose_model,
        )
    
    download_data >> [train_model_A, train_model_B] >> choose_model
    

choose_model_dag()
    