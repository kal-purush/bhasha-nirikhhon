import dbcreds
import mariadb

username = input("Please type a username: ")
content = input("Add blog content: ")

conn = mariadb.connect(user = dbcreds.user, password = dbcreds.password, host = dbcreds.host, port = dbcreds.port, database = dbcreds.database)
cursor = conn.cursor()

cursor.execute("INSERT INTO blog_post(username, content, id) VALUES(?, ?, NULL)", [username, content])
conn.commit()
print("Your blog post has been added!")
cursor.close()
conn.close()

conn = mariadb.connect(user = dbcreds.user, password = dbcreds.password, host = dbcreds.host, port = dbcreds.port, database = dbcreds.database)
cursor = conn.cursor()
cursor.execute("SELECT * FROM blog_post")
rows = cursor.fetchall()
print(rows)
cursor.close()
conn.close()