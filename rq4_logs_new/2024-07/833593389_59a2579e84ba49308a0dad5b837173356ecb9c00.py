import os
from pathlib import Path

import pandas as pd
import pandera as pa
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
        df_crm = pd.read_sql(query, conn)

    return df_crm


if __name__ == "__main__":
    query = "SELECT * FROM produtos_bronze"
    df_crm = extract(query=query)
    schema_crm = pa.infer_schema(df_crm)

    with open("schema_crm.py", "w", encoding="utf-8") as file:
        file.write(schema_crm.to_script())

    print(df_crm)
