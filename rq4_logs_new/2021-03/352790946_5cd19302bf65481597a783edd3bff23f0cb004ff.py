from flask_jsonpify import jsonpify
from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def home():
    #print(jsonpify({'output':'Hello World'}))
    return jsonpify({'output':'Hello World'})

if __name__ == "__main__":
    app.run()
