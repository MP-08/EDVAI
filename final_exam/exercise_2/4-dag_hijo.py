from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Argumentos
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Creación del DAG
with DAG(
    dag_id="dag_hijo",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False, 
) as dag:
    # Tarea 1: Transformación de datos
    transformacion = BashOperator(
        task_id="transformacion",
        bash_command="/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation_final_ej2.py",
    )

    # Tarea 2: Finaliza el proceso
    finaliza_proceso = DummyOperator(task_id="finaliza_proceso")

    # Flujo de tareas
    transformacion >> finaliza_proceso
