from flask import Flask, request, jsonify
from models import db, User, Book, Interaction
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
db.init_app(app)

@app.route('/')
def home():
    return "PickABook API is running!"

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    new_user = User(username=data['username'], email=data['email'])
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"message": "User registered successfully!"})

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)