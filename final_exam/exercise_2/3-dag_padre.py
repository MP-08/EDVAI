from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id="dag_padre",  # Corrección del nombre del parámetro
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Ejecución diaria a medianoche
    start_date=days_ago(2),  # Corrección de `start_date`
    dagrun_timeout=timedelta(minutes=60),
    catchup=True,
) as dag:
    # Inicio del DAG
    inicia_proceso = DummyOperator(task_id="inicia_proceso")

    # Descarga e ingesta de datos
    download_and_ingest = BashOperator(
        task_id="download_and_ingest",
        bash_command="/usr/bin/sh /home/hadoop/scripts/ingest_final_ej1.sh",
    )

    # Dispara el DAG hijo
    trigger_dag_hijo = TriggerDagRunOperator(
        task_id="trigger_dag_hijo",
        trigger_dag_id="dag_hijo",  # Nombre del DAG hijo a disparar
        conf={"execution_date": "{{ ds }}"},  # Parámetro opcional: conf
        reset_dag_run=True,
        wait_for_completion=True,  # Espera la finalización del DAG hijo
        poke_interval=30,  # Tiempo entre intentos de comprobación
    )

    # Secuencia de tareas
    inicia_proceso >> download_and_ingest >> trigger_dag_hijo