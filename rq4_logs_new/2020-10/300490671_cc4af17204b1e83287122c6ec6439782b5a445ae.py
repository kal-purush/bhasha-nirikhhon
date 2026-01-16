
from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Servidor Flask Practica 8 Activo 201504100'