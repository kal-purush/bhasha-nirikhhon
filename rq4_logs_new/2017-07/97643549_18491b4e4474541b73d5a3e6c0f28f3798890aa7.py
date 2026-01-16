import sqlite3

def connect_db():
    conn= sqlite3.connect("books.db")
    cur= conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS book (id INTEGER PRIMARY KEY, title TEXT, author TEXT, year INTEGER, isbn INTEGER)")
    conn.commit()
    conn.close()

def insert(title, author, year, isbn):
    conn= sqlite3.connect("books.db")
    cur= conn.cursor()
    cur.execute("INSERT INTO book VALUES (NULL, ?, ?, ?, ?) ", (title, author, year, isbn))
    conn.commit()
    conn.close()

def view():
    conn= sqlite3.connect("books.db")
    cur= conn.cursor()
    cur.execute("SELECT * FROM book")
    rows=cur.fetchall()
    conn.close()
    return rows

def search(title="", author="", year="", isbn=""): #Le pasamos parametros de busqueda para el filtro y cadenas vacias para que no retorne ningun error
    conn=sqlite3.connect("books.db")
    cur=conn.cursor()
    cur.execute("SELECT * FROM book WHERE title=? OR author=? OR year=? OR isbn=?", (title, author, year, isbn))
    rows=cur.fetchall()
    conn.close()
    return rows

def delete(id): #Se pasa el parametro id ya que el registro sera eliminado por ese argumento
    conn= sqlite3.connect("books.db")
    cur= conn.cursor()
    cur.execute("DELETE FROM book WHERE id=?",(id,))
    conn.commit()
    conn.close()

def update(id, title, author, year, isbn):
    conn= sqlite3.connect("books.db")
    cur= conn.cursor()
    cur.execute("UPDATE book SET title=?, author=?, year=?, isbn=? WHERE id=?",(title, author, year, isbn, id))
    conn.commit()
    conn.close()

connect_db()
#insert("The Lord of the Rings", "J.R.R Tolkien", 1942, 3344348)
#delete(3)
#update(2, "The lord of the rings", "JRR Tolkien", 1956, 64646)
#print(view())
#print(search("The Vampire Diaries"))