# app/__init__.py
from flask import Flask
from config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_principal import Principal
import secrets

# Create instances of Flask extensions
db = SQLAlchemy()
login_manager = LoginManager()
principal = Principal()


def create_app():
    # Initialize the Flask application
    app = Flask(__name__)
    app.secret_key = secrets.token_hex(16)
    app.config.from_object(Config)

    # Initialize the SQLAlchemy instance with the app
    db.init_app(app)

    # Initialize login manager and principal
    login_manager.init_app(app)
    principal.init_app(app)

    # Register blueprints or other configurations
    from .routes import bp as main_bp  # Import route definitions
    app.register_blueprint(main_bp)

    return app


# Define the user_loader callback
from .models import User

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))