import sqlite3

with sqlite3.connect("PhoneBook.db") as db:
    cursor = db.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS Names (
    ID integer PRIMARY KEY AUTOINCREMENT,
    First_Name text NOT NULL,
    Surname text NOT NULL,
    Phone_Number blob NOT NULL)""")

data = [
    ("1", "Simon", "Howeis", "01223 349752"),
    ("2", "Karen", "Phillips", "01954 295773"),
    ("3", "Darren", "Smith", "01583 749012"),
    ("4", "Anne", "Jones", "01323 567322"),
    ("5", "Mark", "Smith", "01223 855534")
]

cursor.executemany("""INSERT INTO Names (ID, First_Name, Surname, Phone_Number)
    VALUES(?, ?, ?, ?)""", data)

db.commit()
db.close()