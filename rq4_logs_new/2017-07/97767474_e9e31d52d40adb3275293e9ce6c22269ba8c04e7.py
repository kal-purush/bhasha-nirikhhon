from flask import Flask
from flask_jwt import JWT

from flask_rest_api.resources.item import Item, ItemList
from flask_rest_api.resources.store import Store, StoreList
from flask_rest_api.resources.user import UserRegistration
from flask_rest_api.security import authenticate, identity


def create_app(uri):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = uri # ?
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.secret_key = "TOP SECRET"

    from flask_rest_api.resources import api
    api.add_resource(Item, '/items/<string:name>')
    api.add_resource(ItemList, '/items')
    api.add_resource(Store, '/stores/<string:name>')
    api.add_resource(StoreList, '/stores')
    api.add_resource(UserRegistration, '/register')
    api.init_app(app)

    jwt = JWT(app, authenticate, identity)
    jwt.init_app(app)

    from flask_rest_api.db import db

    @app.before_first_request
    def create_tables():
        # SqlAlchemy creates tables that it SEES by imports
        db.create_all()

    db.init_app(app)
    return app