from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/libros.db'
db = SQLAlchemy(app)

class Book(db.Model):
    id = db.Column(db.Integer, nullable=False, primary_key=True)
    title = db.Column(db.String(96), unique=True)

@app.route("/")
def home():
    return jsonify({
        "Home": "My Book-Api"
        
    })

@app.route("/books/", endpoint="nuevo_libro", methods=["POST"])
def add_book():
    from flask import request
    json = request.get_json()
    title = json.get("title")
    new_book = Book()
    new_book.title = title
    
    db.session.add(new_book)
    db.session.commit()

    return jsonify({"Book_id": new_book.id}), 201

@app.route("/books/", endpoint="lista_libros", methods=["GET"])
def list_books():
    from flask import request
    limite = int(request.args.get("limit", 10))
    books = Book.query.order_by(Book.id).limit(limite).all()

    return jsonify({
        "Books": [{"id": x.id, "title": x.title} for x in books]
    })

@app.route("/update/", methods=["POST"])
def update():
    from flask import request
    json = request.get_json()
    new_title = json.get("new_title")
    old_title = json.get("old_title")
    book = Book.query.filter_by(title=old_title).first()
    book.title = new_title
    db.session.commit()
    return jsonify({
        "Book_Id": book.id, 
        "State": "Book title Updated Correctly."
        
    })

@app.route("/delete/", methods=["POST"])
def delete():
    from flask import request
    json = request.get_json()
    title = json.get("title")
    book = Book.query.filter_by(title=title).first()
    db.session.delete(book)
    db.session.commit()
    return jsonify({
        "Book_Id": book.id, 
        "State": "The book has been deleted."
        
    })
if __name__ == "__main__":
    db.create_all() # Creamos todas las tablas de la base de datos
    app.run(port=3000, host="0.0.0.0")