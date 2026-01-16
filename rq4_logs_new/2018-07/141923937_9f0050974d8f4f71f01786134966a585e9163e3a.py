from flask import Flask
from config.settings import configure
from flask import Blueprint
from flask_restful import Api
from routes import init_routes
from resources import globalOnlineUsers

def create_app(config_env):
    app = Flask(__name__)

    configure(app, config_env)
    api_bp = Blueprint('api', __name__)
    api = Api(api_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    init_routes(api)

    from models import db
    db.init_app(app)

    globalOnlineUsers.run()
    return app