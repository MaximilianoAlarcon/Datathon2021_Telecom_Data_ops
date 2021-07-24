#Importacion 
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import proceso.functions.functions as script

#Crear DAG
dag = DAG(
    dag_id = "ETL",
    start_date = datetime(2021,1,1),
    schedule_interval = None
)

#Definir tarea/s
t1 = PythonOperator(
    task_id = "Task1",
    python_callable=script.cargar_resultado_en_csv,
    dag=dag,
)

#Definir el flujo de las tareas
t1