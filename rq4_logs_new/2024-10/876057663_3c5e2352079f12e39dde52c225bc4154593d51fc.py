import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def query_to_df(sql):
    connection_string = "mysql+pymysql://root:" + DB_PASSWORD + "@localhost/" + DB_NAME
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        query = text(sql)
        result = connection.execute(query)
        return pd.DataFrame(result.all())
    