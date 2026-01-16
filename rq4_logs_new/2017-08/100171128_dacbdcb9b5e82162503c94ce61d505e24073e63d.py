import sqlite3
from datetime import datetime

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Row(object):
    def __init__(self, row):
        self.row = row
        self.row_number = -1
    def get(self):
        self.row_number += 1
        return self.row[self.row_number]
    def get_date(self):
        s = self.get()
        if s is None:
            return None
        return datetime.strptime(s, '%Y-%m-%d')
    def get_float(self):
        return float(self.get())
    def get_string(self):
        return str(self.get())

class Statement(object):
    def __init__(self, cursor, query):
        self.cursor = cursor
        self.query = query
    def execute(self, params=None):
        if params is not None:
            self.cursor.execute(self.query, params)
        else:
            self.cursor.execute(self.query)
    def next(self):
        r = self.cursor.fetchone()
        if r is not None:
            return Row(r)
        return None

class Database(object, metaclass=Singleton):
    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name)
    def statement(self, query):
        cursor = self.connection.cursor()
        s = Statement(cursor, query)
        return s
    def commit(self):
        self.connection.commit()
    def rollback(self):
        self.connection.rollback()