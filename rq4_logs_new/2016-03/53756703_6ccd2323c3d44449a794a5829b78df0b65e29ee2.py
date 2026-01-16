# coding: utf8
from decimal import Decimal
from config import config
from flask import Flask
from flask.ext.login import LoginManager
from flask.ext.moment import Moment
from flask.ext.sqlalchemy import SQLAlchemy
__author__ = 'py'


def dec(value):
    return float(Decimal(value).quantize(Decimal('0.01')))

db = SQLAlchemy()
moment = Moment()
login_manager = LoginManager()
login_manager.login_message = u"Пожалуйста, зарегистрируйтесь, чтобы попасть на эту страницу."
login_manager.session_protection = 'strong'
login_manager.login_view = 'auth.login'


def create_app(conf_name):
    app = Flask(__name__)
    app.config.from_object(config[conf_name])
    config[conf_name].init_app(app)
    db.init_app(app)
    moment.init_app(app)
    login_manager.init_app(app)

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    from .boss import boss as boss_blueprint
    app.register_blueprint(boss_blueprint, url_prefix='/boss')

    from .admin import admin as admin_blueprint
    app.register_blueprint(admin_blueprint, url_prefix='/admin')

    return app