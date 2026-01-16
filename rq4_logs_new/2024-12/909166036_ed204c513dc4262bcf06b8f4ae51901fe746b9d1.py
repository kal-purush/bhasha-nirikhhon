import logging
import mlflow

from config import settings


def log_to_mlflow():
    print(f"LOGGING TO: {settings.MLFLOW_ENDOINT}")
    mlflow.set_tracking_uri(settings.MLFLOW_ENDOINT)

    try:
        mlflow.create_experiment(name="mandingo")
    except mlflow.exceptions.MlflowException as e:
        print(e)
        logging.warning("Using previous experiment ...")

    with mlflow.start_run(
        experiment_id=mlflow.get_experiment_by_name("mandingo").experiment_id
    ):
        print("Hello from nowhere")