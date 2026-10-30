"""Callbacks de correo para los DAGs.

El enunciado pide que al terminar el entrenamiento llegue un correo con la
fecha de ejecucion y el estado. Se usan como on_success_callback y
on_failure_callback a nivel de DAG, igual que en el ejemplo de clase.
"""

import os
from airflow.utils.email import send_email

DESTINATARIO = os.environ.get("NOTIFICATION_EMAIL", "orodrigohr@gmail.com")


def _enviar(context, estado):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    fecha = context.get("logical_date") or context.get("execution_date")

    asunto = f"Airflow - {dag_id} - {estado}"
    cuerpo = (
        f"<h3>DAG: {dag_id}</h3>"
        f"<p><b>Estado:</b> {estado}</p>"
        f"<p><b>Fecha de ejecucion:</b> {fecha}</p>"
        f"<p><b>Ultima tarea:</b> {ti.task_id}</p>"
        f"<p><b>Log:</b> <a href='{ti.log_url}'>{ti.log_url}</a></p>"
    )
    send_email(to=DESTINATARIO, subject=asunto, html_content=cuerpo)


def correo_exito(context):
    _enviar(context, "EXITOSO")


def correo_fallo(context):
    _enviar(context, "FALLIDO")
