"""DAG 1 - Entrenamiento del modelo.

Parte 1 de la asignacion. Entrena el Lasso con los CSV de entrada y envia un
correo al terminar, indicando la fecha de ejecucion y si el proceso fue
exitoso o fallido.
"""

import sys
from datetime import datetime, timedelta

from airflow.decorators import dag

PATH_COMMON = "/opt/airflow/dags"
sys.path.append(PATH_COMMON)

from common.add_task import task_entrenar_modelo
from common.notificaciones import correo_exito, correo_fallo

default_args = {
    "owner": "omar.huaman",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="dag_entrenamiento_modelo",
    description="Entrena el modelo de prediccion de precios de vivienda",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["mlops17", "modulo5", "entrenamiento"],
    on_success_callback=correo_exito,
    on_failure_callback=correo_fallo,
)
def dag_entrenamiento():
    task_entrenar_modelo()


dag_entrenamiento()
