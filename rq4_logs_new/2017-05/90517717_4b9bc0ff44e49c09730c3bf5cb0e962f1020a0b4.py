#!flask/bin/python
from flask import Flask, abort, request, jsonify, make_response
import redis 

app = Flask(__name__)

'''
books = {
    '1':{
        'isbn': 1,
        'title': u'Learn Java',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'author': u'author-1',
        'category': u'Comic',
        'price': 10.5
    },
    '2':{
        'isbn': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'author': u'author-2',
        'category': u'Comic',
        'price': 10.5
    }
}
'''

#this will be an env coming from the docker host while starting the container
r = redis.Redis(host='localhost')

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad input data'}), 400)

@app.route('/library/v1.0/books', methods=['GET'])
def get_books():
    books = {}
    for key in r.keys():
        books[key] = r.get(key)
    return jsonify(books)

@app.route('/library/v1.0/books/<int:isbn>', methods=['GET'])
def get_book(isbn):
    #print str(isbn)
    #print books
    #print 'hello'
    if not r.get(str(isbn)):
        abort(404)
    return jsonify(r.get(str(isbn)))

@app.route('/library/v1.0/books', methods=['POST'])
def add_book():
    if not request.json or not 'isbn' in request.json or not 'title' in request.json or not 'author' in request.json or not 'price' in request.json:
        abort(400)
    isbn = request.json['isbn']
    if r.get(str(isbn)):
        abort(404)
    else:
    	book = {
        	'isbn': isbn,
        	'title': request.json['title'],
        	'description': request.json.get('description', ""),
        	'author': request.json['title'],
        	'category': request.json.get('category',""),
        	'price': request.json['price']
    	}
    	r.set(str(isbn),book)
    	return jsonify(book), 201

@app.route('/library/v1.0/books/<int:isbn>', methods=['PUT'])
def update_book(isbn):
    print isbn
    if not r.get(str(isbn)):
        abort(404)
    if not request.json:
        print 'request.json'
        abort(400)
    if 'title' in request.json and type(request.json['title']) != unicode:
        print 'request.json.title'
        abort(400)
    if 'description' in request.json and type(request.json['description']) is not unicode:
        print 'request.json.description'
        abort(400)
    #if 'price' in request.json and type(request.json['price']) is not unicode:
    #    print 'request.json.price'
    #    abort(400)
    if 'category' in request.json and type(request.json['category']) is not unicode:
        print 'request.json.category'
    	abort(400)

    r.delete(str(isbn))
    book = {
       	'isbn': isbn,
       	'title': request.json['title'],
       	'description': request.json.get('description', ""),
       	'author': request.json['title'],
       	'category': request.json.get('category',""),
       	'price': request.json['price']
    }
    r.set(str(isbn),book)
    return jsonify(book)

@app.route('/library/v1.0/books/<int:isbn>', methods=['DELETE'])
def delete_book(isbn):
    if not r.get(str(isbn)):
        abort(404)
    r.delete(isbn)
    return jsonify({'result': True})    

if __name__ == '__main__':
    app.run(debug=True)