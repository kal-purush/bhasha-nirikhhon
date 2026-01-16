from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# Define and initialize our app as an instance of Flask class.

app.config.from_object('config')
# Tell our newly created app to load configuration from config.py.

db = SQLAlchemy(app)
# Define and initialize a database as an instance of SQLAlchemy class, linked to our app.
# We are going to define our models in models.py

from app import views