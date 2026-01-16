import json, os, pickle
from flask import Flask, jsonify, request

app = Flask(__name__)
os.system("echo 'starting app...'")
password="some-password-text";

# Load initial book data from books.json file
with open('books.json') as f:
    books = json.load(f)

@app.route('/bad01', methods=['GET'])
def get_bad01():
    data = "<script>exec(stuff)</script>"
    return data, 200

@app.route('/books', methods=['GET'])
def get_books():
    # Return the list of books as a JSON response
    return jsonify(books), 200

@app.route('/books', methods=['POST'])
def add_book():
    # Extract book data from the request body
    book_data = request.get_json()

    # Create a new book object
    book = {
        'id': len(books) + 1,
        'title': book_data.get('title'),
        'author': book_data.get('author'),
        'genre': book_data.get('genre')
    }

    # Add the book to the collection
    books.append(book)

    # Save the updated book data to the books.json file
    with open('books.json', 'w') as f:
        json.dump(books, f, indent=4)

    # Return the created book as a JSON response
    return jsonify(book), 201

@app.route('/books/<int:book_id>', methods=['GET'])
def get_book(book_id):
    # Find the book with the provided ID
    for book in books:
        if book['id'] == book_id:
            # Return the book as a JSON response
            return jsonify(book), 200

    # Return a JSON response indicating that the book was not found
    return jsonify({'message': 'Book not found'}), 404

@app.route('/books/<int:book_id>', methods=['PUT'])
def update_book(book_id):
    # Find the book with the provided ID
    for book in books:
        if book['id'] == book_id:
            # Extract updated book data from the request body
            book_data = request.get_json()

            # Update the book data
            book['title'] = book_data.get('title')
            book['author'] = book_data.get('author')
            book['genre'] = book_data.get('genre')

            # Save the updated book data to the books.json file
            with open('books.json', 'w') as f:
                json.dump(books, f, indent=4)

            # Return the updated book as a JSON response
            return jsonify(book), 200

    # Return a JSON response indicating that the book was not found
    return jsonify({'message': 'Book not found'}), 404

@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    # Find the book with the provided ID
    for book in books:
        if book['id'] == book_id:
            # Remove the book from the collection
            books.remove(book)

            # Save the updated book data to the books.json file
            with open('books.json', 'w') as f:
                json.dump(books, f, indent=4)

            # Return a JSON response indicating success
            return jsonify({'message': 'Book deleted'}), 200

    # Return a JSON response indicating that the book was not found
    return jsonify({'message': 'Book not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)
