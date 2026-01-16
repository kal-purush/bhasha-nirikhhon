import sqlite3

file = 'pbase.db'
connection = sqlite3.connect(file)
print(connection)

cursor = connection.cursor()
query = "select * from petbase"

cursor.execute(query)
result = cursor.fetchall()
print(result)