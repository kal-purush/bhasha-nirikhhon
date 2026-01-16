from sqlalchemy import create_engine
import pandas as pd
from config import db_params, postgreSQL_db_name

def create_datamart(db: str, table_name: str):
    jdbc_url = (f"postgresql://{db_params["user"]}:{db_params["password"]}@"
                f"{db_params["host"]}:{db_params["port"]}/{db}")
    engine = create_engine(jdbc_url)
    query = open("datamart_script.sql", 'r')
    df = pd.read_sql_query(query.read(), engine)
    df.to_sql(table_name, engine, if_exists="append")

if __name__ == "__main__":
    create_datamart(postgreSQL_db_name, "datamart")