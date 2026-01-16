#----------------------------------------------------------------------------------------
# import packages
#----------------------------------------------------------------------------------------
from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd
import psycopg2
import psycopg2.extras

#----------------------------------------------------------------------------------------
# load environment variables
#----------------------------------------------------------------------------------------
load_dotenv()
loading_dir:str|None = os.getenv('LOADING_DIR')
db_host:str|None = os.getenv('DB_HOST')
db_port:str|None = os.getenv('DB_PORT')
db:str|None = os.getenv('DB')
db_user:str|None = os.getenv('DB_USER')
db_pw:str|None = os.getenv('DB_PW')

#----------------------------------------------------------------------------------------
# setup vars and db connection
#----------------------------------------------------------------------------------------
db_files:list[str] = os.listdir(loading_dir)
psql_conn = psycopg2.connect( host=db_host, database=db, user=db_user, password=db_pw, port=db_port)
psql_conn.autocommit = True
psql_cur = psql_conn.cursor()

#----------------------------------------------------------------------------------------
# define and call load method
#----------------------------------------------------------------------------------------
def load_db_file(filename:str):
    # Connect to the SQLite database
    conn = sqlite3.connect(os.path.join(str(loading_dir), filename))

    # Create a cursor object
    cursor = conn.cursor()

    # List all tables in the database
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")

    # Fetch all rows returned by the query
    tables = cursor.fetchall()

    # Print the table names
    for table in tables:
        sql: str = f"drop table if exists staging.{table[0]}"
        psql_cur.execute(sql)



load_db_file("journeys_nzta_20240907100303.db")