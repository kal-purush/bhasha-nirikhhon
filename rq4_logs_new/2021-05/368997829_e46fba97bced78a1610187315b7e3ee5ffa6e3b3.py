from flask import Flask, jsonify
from flask_cors import CORS
from fridge_data import fridge_data

app = Flask(__name__)
CORS(app)


@app.route('/fridge-stats', methods=['GET'])
def get_fridge_data():
    return jsonify(fridge_data)