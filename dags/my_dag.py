from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

def _escojer_el_mejor_modelo(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'modelo_de_entrenamiento_A',
        'modelo_de_entrenamiento_B',
        'modelo_de_entrenamiento_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'precisa'
    return 'imprecisa'


def _mejor_modelo():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        modelo_de_entrenamiento_A = PythonOperator(
            task_id="modelo_de_entrenamiento_A",
            python_callable=_mejor_modelo
        )

        modelo_de_entrenamiento_B = PythonOperator(
            task_id="modelo_de_entrenamiento_B",
            python_callable=_mejor_modelo
        )

        modelo_de_entrenamiento_C = PythonOperator(
            task_id="modelo_de_entrenamiento_C",
            python_callable=_mejor_modelo
        )

        escojer_el_mejor_modelo = BranchPythonOperator(
            task_id="escojer_el_mejor_modelo",
            python_callable=_escojer_el_mejor_modelo
        )

        precisa = BashOperator(
            task_id="precisa",
            bash_command="echo 'precisa'"
        )

        imprecisa = BashOperator(
            task_id="imprecisa",
            bash_command="echo 'imprecisa'"
        )

        [modelo_de_entrenamiento_A, modelo_de_entrenamiento_B, modelo_de_entrenamiento_C] >> escojer_el_mejor_modelo >> [precisa, imprecisa]
        
        