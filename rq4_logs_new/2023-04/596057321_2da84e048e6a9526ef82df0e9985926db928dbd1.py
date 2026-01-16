import logging
import os
import datetime as dt
from src.load import load
from src.extract import extract
from src.transform import transformation


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Executes the ETL process for processing polling events, connectivity status, and orders data.

    Args:
        None

    Returns:
        None
    """

    # Use default input path
    filename_polling = "data/input/polling.csv"
    filename_conn_status = "data/input/connectivity_status.csv"
    filename_orders = "data/input/orders.csv"
    filename_output = f"data/output/events_report_{dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    # Create input/output directory if it doesn't exist
    file_paths = [
        filename_polling,
        filename_conn_status,
        filename_orders,
        filename_output,
    ]
    for fp in file_paths:
        os.makedirs(os.path.dirname("/".join(fp.split("/")[:-1])), exist_ok=True)

    # extract
    logger.info("Starting extraction step")
    df_polling, df_conn_status, df_orders = extract(
        path_polling=filename_polling,
        path_conn_status=filename_conn_status,
        path_orders=filename_orders,
    )
    logger.info("DONE: extraction step")

    # transform
    logger.info("Starting transformation step")
    data_transform = transformation(
        df_polling=df_polling,
        df_connectivity_status=df_conn_status,
        df_orders=df_orders,
    )
    logger.info("DONE: transformation step")

    # load
    logger.info("Starting load step")
    ret, output_path = load(df=data_transform, path_to_write=filename_output)
    logger.info("DONE: load done")
    logger.info(f"FINISHED: Output data path is {output_path}")


if __name__ == "__main__":
    main()