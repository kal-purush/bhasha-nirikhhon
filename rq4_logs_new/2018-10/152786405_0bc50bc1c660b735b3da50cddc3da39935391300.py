import os
import psycopg2

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB_NAME = os.getenv("PG_DB_NAME")


def init_db():
    db_conn = connect()
    cursor = db_conn.cursor()

    # Create a table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "id serial PRIMARY KEY, "
        "username text NOT NULL UNIQUE, "
        "first_name text NOT NULL, "
        "last_name text NOT NULL);"
    )
    db_conn.commit()
    close_connection(db_conn, cursor=cursor)


def close_connection(connection, cursor=False):
    if cursor:
        cursor.close()
    connection.close()


def connect():
    c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                         password=PG_PASSWORD, dbname=PG_DB_NAME)
    return c