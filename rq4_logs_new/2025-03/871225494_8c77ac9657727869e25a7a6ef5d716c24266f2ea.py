import sqlite3
from datetime import datetime

DB_NAME = "database.db"

class Parent():
    def __init__(self, name:str=DB_NAME) -> None:
        self.connection = sqlite3.connect(name)
        self.cursor = self.connection.cursor()
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS parents (
                user_id INTEGER PRIMARY KEY,
                telegram_id INTEGER,
                username TEXT NOT NULL,
                fio TEXT NOT NULL,
                phone TEXT NOT NULL,
                date_at TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS tg_uniq_idx ON parents(telegram_id)
        ''')
        self.connection.commit()
    
    def find_by_tg_id(self, telegram_id:int):
        self.cursor = self.connection.cursor()
        self.cursor.execute('SELECT user_id FROM parents WHERE telegram_id =?', (telegram_id,))
        row = self.cursor.fetchone()
        if row:
            return row[0]

    def insert(self, telegram_id:int, username:str, fio:str, phone:str) -> None:
        date = datetime.now().strftime('%d/%m/%Y, %H:%M')
        self.cursor = self.connection.cursor()
        self.cursor.execute('''
INSERT INTO parents (telegram_id, username, fio, phone, date_at) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(telegram_id) DO UPDATE SET 
    username=excluded.username,
    fio=excluded.fio,
    phone=excluded.phone
''', 
            (telegram_id, username, fio, phone, date))
        self.connection.commit()
    
    def __del__(self):
        self.connection.close()


class Children():
    def __init__(self, name:str=DB_NAME) -> None:
        self.connection = sqlite3.connect(name)
        self.cursor = self.connection.cursor()
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS children (
                child_id INTEGER PRIMARY KEY,
                fio TEXT NOT NULL,
                parent_id INTEGER,
                group_id INTEGER
            );
        ''')
        self.connection.commit()
                    
    def insert(self, fio:str, parent_id:int, group_id:int) -> None:
        self.cursor = self.connection.cursor()
        self.cursor.execute('''
INSERT INTO children (fio, parent_id, group_id) VALUES (?, ?, ?)
''', 
            (fio, parent_id, group_id))
        self.connection.commit()
    
    def __del__(self):
        self.connection.close()
