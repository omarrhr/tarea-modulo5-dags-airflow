# Asignación módulo 5 — DAGs de entrenamiento y predicción en Airflow

Omar Rodrigo Huaman Rivera · DataPath, MLOps Engineer 17ª Edición

Dos DAGs de Airflow sobre el modelo de predicción de precios de vivienda
(Ames Housing), el mismo que se trabajó en las sesiones del módulo.

---

## 1. Qué hace cada DAG

### `dag_entrenamiento_modelo`

Entrena el modelo y lo serializa.

| Tarea | Qué hace |
|---|---|
| `entrenar_modelo` | Lee `xtrain.csv` y `ytrain.csv`, filtra las 36 variables de `selected_features.csv`, entrena un `Lasso(alpha=0.001)` y guarda `linear_regression.joblib` |

Al terminar envía un **correo con la fecha de ejecución y el estado**, tanto si
el proceso fue exitoso como si falló.

### `dag_prediccion_modelo`

Genera las predicciones. Son **dos tareas encadenadas**, como pide el enunciado.

| Tarea | Qué hace |
|---|---|
| `procesar_datos` | Lee `xtest.csv`, valida que estén las 36 columnas, las filtra, añade la fecha de proceso y escribe `datos_procesados.csv`. Devuelve la ruta por XCom |
| `predecir` | Recibe esa ruta, carga el `.joblib`, predice y guarda `predictions.csv` |

La segunda tarea consume el resultado de la primera a través de XCom, de modo
que la dependencia es real y no solo de orden.

Las predicciones se guardan con el logaritmo que devuelve el modelo y el precio
reconstruido en dólares, más la fecha de proceso y la de carga.

---

## 2. Estructura

```
dags/
  dag_entrenamiento.py          DAG 1
  dag_prediccion.py             DAG 2
  common/
    add_task.py                 las tres tareas
    notificaciones.py           callbacks de correo
  data/
    inputs/                     xtrain, ytrain, xtest, ytest, selected_features
    model/                      aquí se escribe el .joblib
    outputs/                    aquí salen los CSV de resultado
docker-compose.yaml             entorno de Airflow 2.9.3
.env.ejemplo                    plantilla de configuración
```

Cada tarea corre en su propio virtualenv (`@task.virtualenv`), igual que en el
ejemplo de clase, para que las versiones de scikit-learn no choquen con las de
Airflow.

---

## 3. Cómo ejecutarlo

```bash
cp .env.ejemplo .env
```

Edita `.env` y pon la contraseña de aplicación de Gmail en
`AIRFLOW__SMTP__SMTP_PASSWORD`. Se genera en
`https://myaccount.google.com/apppasswords` — la contraseña normal del correo
no funciona.

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow queda en `http://localhost:8080`, usuario y contraseña `airflow`.

Ejecuta primero `dag_entrenamiento_modelo`, porque la predicción necesita el
`.joblib` que genera. Después `dag_prediccion_modelo`.

Para apagarlo:

```bash
docker compose down
```

---

## 4. Notificación por correo

El enunciado pide avisar al terminar el entrenamiento con la fecha de ejecución
y el estado. Está resuelto con `on_success_callback` y `on_failure_callback` a
nivel de DAG, que llaman a `send_email` de Airflow.

El correo incluye el nombre del DAG, el estado, la fecha de ejecución, la última
tarea y el enlace al log.

El servidor SMTP se configura por variables de entorno en `docker-compose.yaml`,
que las toma del `.env`. Está puesto para Gmail; para otro proveedor solo
cambian el host y el puerto.
