from flask import Flask, make_response, jsonify


app = Flask(__name__)

@app.route("/greeting", methods=["GET"], strict_slashes=False)
def get_greeting():
    return make_response(jsonify({"hello": "world"}), 200)