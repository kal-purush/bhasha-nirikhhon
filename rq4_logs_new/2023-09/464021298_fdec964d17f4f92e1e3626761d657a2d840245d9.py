import sqlite3

from datetime import datetime
from moba_insert_data import get_mlbb_hero_data

# SQLiteデータベースに接続
conn = sqlite3.connect('moba_database.sqlite3')
cursor = conn.cursor()

# mlbbのデータを保存するテーブルを作成
cursor.execute('''
  CREATE TABLE IF NOT EXISTS term (
    name_en TEXT UNIQUE,
    name_ja TEXT UNIQUE,
    created_at TEXT,
    updated_at TEXT
  )
''')