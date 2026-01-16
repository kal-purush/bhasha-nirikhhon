import os
from flask import Flask
from config import Config

def create_app():
    app = Flask("Accu TD")
    app.config.from_object(Config)

    from app import routes
    app.register_blueprint(routes.main)
    uploads_dir = os.path.join(app.root_path, 'static', 'uploads')
    os.makedirs(uploads_dir, exist_ok=True) 

    return app