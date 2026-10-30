"""DAG 2 - Prediccion del modelo.

Parte 2 de la asignacion. Dos tareas encadenadas: la primera procesa los
datos de entrada y la segunda consume ese resultado y ejecuta la prediccion.
"""

import sys
from datetime import datetime, timedelta

from airflow.decorators import dag

PATH_COMMON = "/opt/airflow/dags"
sys.path.append(PATH_COMMON)

from common.add_task import task_procesar_datos, task_predecir
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
    dag_id="dag_prediccion_modelo",
    description="Procesa los datos de entrada y genera las predicciones",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["mlops17", "modulo5", "prediccion"],
    on_success_callback=correo_exito,
    on_failure_callback=correo_fallo,
)
def dag_prediccion():
    # Task 1 devuelve la ruta del archivo procesado; Task 2 la recibe por XCom.
    ruta = task_procesar_datos()
    task_predecir(ruta)


dag_prediccion()
