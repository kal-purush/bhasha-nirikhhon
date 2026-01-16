from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)  # Allow requests from Flutter

# Connect to MongoDB
client = MongoClient("mongodb+srv://21uai019:O2OLlukOZ8POqvUg@megaproject-snapattend.tuf5i1s.mongodb.net/")
db = client['SA-General']
faculty_collection = db['Faculty']

# Debug: Print all documents from faculty collection (optional)
for doc in faculty_collection.find():
    print(doc)

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    # Find faculty by email and password
    faculty = faculty_collection.find_one({'email': email, 'password': password})

    if faculty:
        return jsonify({
            'status': 'success',
            'department': faculty.get('department', 'N/A'),
            'name': faculty.get('name', 'Unknown')
        })
    else:
        return jsonify({'status': 'fail', 'message': 'Invalid email or password'}), 401

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")