import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DBConnection:
    def __init__(self):
        self.connection = psycopg2.connect(user="postgres",
                                           password="postgres",
                                           host="127.0.0.1",
                                           port="1946",
                                           database="events")
        self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.connection.cursor()

    def check_existence(self):
        pass

    def __del__(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

# cursor.execute('''create table if not exists events ();''')