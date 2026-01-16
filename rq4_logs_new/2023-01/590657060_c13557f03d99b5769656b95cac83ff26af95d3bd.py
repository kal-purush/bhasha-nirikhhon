import sqlite3

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# ------------ CREATE CONNECTION TO DB
# db = sqlite3.connect("books-collection.db")
# cursor = db.cursor()

# ------------ CREATE NEW TABLE
# cursor.execute("CREATE TABLE books (id INTEGER PRIMARY KEY, title varchar(250) NOT NULL UNIQUE, author varchar(250) NOT NULL, rating FLOAT NOT NULL)")

# ------------ CREATE NEW ENTRY(ROW)
# cursor.execute("INSERT INTO books VALUES(1, 'Harry Potter', 'J. K. Rowling', '9.3')")
# db.commit()

# ------------ CREATE NEW TABLE USING FLASK
app = Flask(__name__)
app.app_context().push()

app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///new-books-collection.db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Book(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(250), unique=True, nullable=False)
    author = db.Column(db.String(250), nullable=False)
    rating = db.Column(db.Float, nullable=False)

    def __repr__(self):
        return f'<Book {self.title}>'


# ------------ CREATE TABLE
db.create_all()


# ------------ CREATE ENTRY
# new_book = Book(title="Harry Potter", author="J. K. Rowling", rating=9.3)
# db.session.add(new_book)
# db.session.commit()

# ------------ READ TABLE
# all_books = Book.query.all()
# print(all_books)


# ------------ EDIT ENTRY BY QUERY(SEARCH)
# book_to_update = Book.query.filter_by(title="Harry Potter").first()
# book_to_update.title = "Harry Potter and the Goblet of Fire"
# db.session.commit()


# ------------ EDIT ENTRY BY PRIMARY KEY
# book_id = 1
# book_to_update = Book.query.get(book_id)
# book_to_update.title = "Harry Potter and the Goblet of Fire"
# db.session.commit()  


# # ------------ DELETE ENTRY BY PRIMARY KEY
# book_id = 1
# book_to_delete = Book.query.get(book_id)
# db.session.delete(book_to_delete)
# db.session.commit()