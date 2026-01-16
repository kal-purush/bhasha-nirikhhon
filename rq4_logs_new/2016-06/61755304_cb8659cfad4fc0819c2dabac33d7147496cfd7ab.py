import sqlite3


# Create a database in RAM
db = sqlite3.connect(':memory:')
# Creates or opens a file called users with a SQLite3 DB
db = sqlite3.connect('users')

# with sqlite3.connect('leave').db as connection:
# 	c = connection.cursor()
# 	c.execute("""CREATE TABLE posts(title TEXT,description TEXT)""")
# 	c.execute('INSERT INTO posts VALUES("good","I\'m good")')
# 	c.execute('INSERT INTO posts VALUES("well","I\'m well")')


# Get a cursor object
cursor = db.cursor()
cursor.execute('''
    CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT,
                       phone TEXT, email TEXT unique, password TEXT)
''')
db.commit()