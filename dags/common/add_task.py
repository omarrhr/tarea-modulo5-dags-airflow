"""Tareas que usan los dos DAGs de la asignacion.

Cada tarea corre en su propio virtualenv, igual que en el ejemplo de clase,
para que las dependencias de scikit-learn no se mezclen con las de Airflow.
Las rutas son absolutas porque dentro del contenedor los DAGs viven en
/opt/airflow/dags.
"""

from airflow.decorators import task

REQS = ["pandas", "numpy", "scikit-learn", "joblib"]

RUTA_INPUTS = "/opt/airflow/dags/data/inputs"
RUTA_MODELO = "/opt/airflow/dags/data/model/linear_regression.joblib"
RUTA_OUTPUTS = "/opt/airflow/dags/data/outputs"


# ---------------------------------------------------------------- DAG 1
@task.virtualenv(task_id="entrenar_modelo", requirements=REQS, system_site_packages=False)
def task_entrenar_modelo():
    """Entrena el Lasso con los CSV de entrada y lo serializa."""
    import pandas as pd
    import joblib
    from sklearn.linear_model import Lasso

    RUTA_INPUTS = "/opt/airflow/dags/data/inputs"
    RUTA_MODELO = "/opt/airflow/dags/data/model/linear_regression.joblib"

    X_train = pd.read_csv(f"{RUTA_INPUTS}/xtrain.csv")
    y_train = pd.read_csv(f"{RUTA_INPUTS}/ytrain.csv")

    features = pd.read_csv(f"{RUTA_INPUTS}/selected_features.csv")["0"].to_list()
    X_train = X_train[features]

    modelo = Lasso(alpha=0.001, random_state=0)
    modelo.fit(X_train, y_train)

    joblib.dump(modelo, RUTA_MODELO)
    print(f"Modelo entrenado con {len(X_train)} filas y {len(features)} variables")
    print(f"Guardado en {RUTA_MODELO}")


# ---------------------------------------------------------------- DAG 2
@task.virtualenv(task_id="procesar_datos", requirements=REQS, system_site_packages=False)
def task_procesar_datos():
    """Task 1: lee los datos de entrada, los filtra y los deja preparados.

    Devuelve la ruta del archivo procesado para que la segunda tarea la
    reciba por XCom.
    """
    import pandas as pd
    from datetime import datetime

    RUTA_INPUTS = "/opt/airflow/dags/data/inputs"
    RUTA_OUTPUTS = "/opt/airflow/dags/data/outputs"

    X = pd.read_csv(f"{RUTA_INPUTS}/xtest.csv")
    features = pd.read_csv(f"{RUTA_INPUTS}/selected_features.csv")["0"].to_list()

    faltantes = [c for c in features if c not in X.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas en la entrada: {faltantes}")

    X = X[features]
    X["fecha_proceso"] = datetime.now().strftime("%Y-%m-%d")

    ruta = f"{RUTA_OUTPUTS}/datos_procesados.csv"
    X.to_csv(ruta, index=False)

    print(f"Procesadas {len(X)} filas con {len(features)} variables")
    return ruta


@task.virtualenv(task_id="predecir", requirements=REQS, system_site_packages=False)
def task_predecir(ruta_procesados: str):
    """Task 2: consume la salida de la tarea anterior y predice."""
    import pandas as pd
    import numpy as np
    from joblib import load
    from datetime import datetime

    RUTA_INPUTS = "/opt/airflow/dags/data/inputs"
    RUTA_MODELO = "/opt/airflow/dags/data/model/linear_regression.joblib"
    RUTA_OUTPUTS = "/opt/airflow/dags/data/outputs"

    datos = pd.read_csv(ruta_procesados)
    fecha = datos.pop("fecha_proceso").iloc[0]

    modelo = load(RUTA_MODELO)
    features = pd.read_csv(f"{RUTA_INPUTS}/selected_features.csv")["0"].to_list()

    pred_log = modelo.predict(datos[features])

    salida = pd.DataFrame({
        "prediction_log": pred_log,
        "precio_usd": np.exp(pred_log).round(2),
        "fecha_proceso": fecha,
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    ruta = f"{RUTA_OUTPUTS}/predictions.csv"
    cabecera = not pd.io.common.file_exists(ruta)
    salida.to_csv(ruta, mode="a", header=cabecera, index=False)

    print(f"Predicciones generadas: {len(salida)} filas -> {ruta}")
    print(salida.head().to_string())
