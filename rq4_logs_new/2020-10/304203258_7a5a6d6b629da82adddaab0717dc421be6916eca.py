import sqlite3 as sql
from sqlite3 import Error
import logging


# def save_connection(connection):
#     try:
#         self.connection.commit()
#     except Exception as e:
#         connection.rollback
#         logging.error(print(e))
#
# def create_connection(self, db_file_path: str) -> None:
#     connection = None
#     try:
#         connection = sql.connect(db_file_path)
#     except Error as e:
#         logging.info(print(e))
#         raise ConnectionError(print(e))
#     return  connection