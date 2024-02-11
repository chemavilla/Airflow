"""
This DAG test XCOM operators comunications by simulating the training 
of two ML models, and chooses the best model based on their accuracy.
"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from default_args import common_args

@dag(dag_id='xcoms',
    start_date=datetime(2024, 2, 1),
    tags=["test"],
    default_args=common_args)
def choose_model_dag():
    def training_model(ti):
        """
        This function is used to pass via XCom the accuracy of an fictional ml model.

        Args:
            ti (TaskInstance): The TaskInstance object.

        Returns:
            None
        """
        import random 
        ti.xcom_push(key='accuracy', value=random.random())

    def choose_model(ti):
        """
        Choose the model with the highest accuracy based on the XCom values.

        Args:
            ti (TaskInstance): The TaskInstance object.

        Returns:
            None
        """
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
            python_callable=training_model)
    
    train_model_B = PythonOperator(
            task_id='train_model_B',
            python_callable=training_model)

    choose_model = PythonOperator(
            task_id='choose_model',
            python_callable=choose_model)
    
    download_data >> [train_model_A, train_model_B] >> choose_model  

choose_model_dag()
    