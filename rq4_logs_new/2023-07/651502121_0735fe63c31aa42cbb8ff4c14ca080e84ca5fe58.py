from typing import List
import psycopg
from settings import (
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
)


class PostgresDB:
    def __init__(self) -> None:
        self.conninfo = f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}"

    def execution_datetime(self) -> None:
        """
        Insert into database the datetime informing the time when the spider started
        """
        with psycopg.connect(self.conninfo) as aconn:
            with aconn.cursor() as cur:
                cur.execute("INSERT INTO executions(datetime) VALUES(NOW())")
            aconn.commit()