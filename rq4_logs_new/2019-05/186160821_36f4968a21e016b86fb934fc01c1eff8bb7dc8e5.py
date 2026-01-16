from flask import Flask
from config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from app.make_celery import make_celery


app = Flask(__name__)
app.config.from_object(Config)

celery = make_celery(app)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

from app import  routes, models