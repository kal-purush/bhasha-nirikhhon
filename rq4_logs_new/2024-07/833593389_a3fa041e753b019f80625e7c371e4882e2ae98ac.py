import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def load_settings():
    dotenv_path = Path.cwd() / ".env"
    load_dotenv(dotenv_path=dotenv_path)

    settings = {
        "db_host": os.getenv("POSTGRES_HOST"),
        "db_user": os.getenv("POSTGRES_USER"),
        "db_pass": os.getenv("POSTGRES_PASSWORD"),
        "db_name": os.getenv("POSTGRES_DB"),
        "db_port": os.getenv("POSTGRES_PORT"),
    }

    return settings


def extract(query: str) -> pd.DataFrame:
    settings = load_settings()

    # create connection string
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"

    # create sqlalchemy engine
    engine = create_engine(connection_string)

    with engine.connect() as conn, conn.begin():
        df = pd.read_sql(query, conn)
        