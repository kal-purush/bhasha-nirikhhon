# app.py

# Required imports
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import json
from firebase_admin import credentials, firestore, initialize_app

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize Firestore DB
cred = credentials.Certificate('key.json')
default_app = initialize_app(cred)
db = firestore.client()
data_ref = db.collection('data')

@app.route('/')
def hello_world():
    return 'ROAD AI API'

@app.route('/add', methods=['POST'])
def create():
    """
        create() : Add document to Firestore collection with request body.
        Ensure you pass a custom ID as part of json body in post request,
        e.g. json={'id': '1', 'title': 'Write a blog post'}
    """
    try:
        print(request.data)
        data_ref.add(json.loads(request.data))
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"An Error Occurred: {e}"

@app.route('/list', methods=['GET'])
def read():
    """
        read() : Fetches documents from Firestore collection as JSON.
        data : Return document that matches query ID.
        all_datas : Return all documents.
    """
    try:
        # Check if ID was passed to URL query
        data_id = request.args.get('id')
        if data_id:
            data = data_ref.document(data_id).get()
            return jsonify(data.to_dict()), 200
        else:
            all_datas = [doc.to_dict() for doc in data_ref.stream()]
            return jsonify(all_datas), 200
    except Exception as e:
        return f"An Error Occurred: {e}"

@app.route('/update', methods=['POST', 'PUT'])
def update():
    """
        update() : Update document in Firestore collection with request body.
        Ensure you pass a custom ID as part of json body in post request,
        e.g. json={'id': '1', 'title': 'Write a blog post today'}
    """
    try:
        id = request.json['id']
        data_ref.document(id).update(request.json)
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"An Error Occurred: {e}"

@app.route('/delete', methods=['GET', 'DELETE'])
def delete():
    """
        delete() : Delete a document from Firestore collection.
    """
    try:
        # Check for ID in URL query
        data_id = request.args.get('id')
        data_ref.document(data_id).delete()
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"An Error Occurred: {e}"

port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0', port=port)