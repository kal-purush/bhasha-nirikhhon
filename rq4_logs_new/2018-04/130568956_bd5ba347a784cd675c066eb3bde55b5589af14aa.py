from os import getcwd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_uploads import UploadSet, configure_uploads, IMAGES

UPLOAD_FOLDER = getcwd() + '/database/images/'


def config_app():
    # default app configuration
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + getcwd() + '/database/api_db.db'
    app.config['SQLALCHEMY_ECHO'] = True
    return app


def config_img_upload(app):
    # image upload configuration
    app.config['UPLOADS_DEFAULT_DEST'] = UPLOAD_FOLDER
    image = UploadSet('images', IMAGES)
    configure_uploads(app, image)
    return image


def config_app_db(app):
    # db configuration
    db = SQLAlchemy(app)
    return db


def init_app():
    app = config_app()
    return app, config_img_upload(app), config_app_db(app)