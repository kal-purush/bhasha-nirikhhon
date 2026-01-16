from flask import Flask
from gallery.config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_assets import Environment, Bundle

app = Flask(__name__)
app.config.from_object(Config)

db = SQLAlchemy(app)
login = LoginManager(app)
login.login_view = 'login'

import gallery.views, gallery.models

assets = Environment(app)

js = Bundle('jquery.js', 'MAgallery.js',
            filters='jsmin', output='gen/packed.js')
assets.register('js_all', js)

css = Bundle('bootstrap.min.css', 'MAgallery.css',
            filters='cssmin', output='gen/packed.css')
assets.register('css_all', css)

# pip install jsmin cssmin
# pip install Flask-Assets

db.create_all()
