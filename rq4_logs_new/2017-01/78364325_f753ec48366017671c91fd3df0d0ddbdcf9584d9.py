from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!This is the Opsgadget Blog from Nacho Stein Lumiserv Homework'