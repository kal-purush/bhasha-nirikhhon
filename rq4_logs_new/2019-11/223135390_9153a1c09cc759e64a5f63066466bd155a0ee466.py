import os
import config
from flask import Flask,render_template
from models.base_model import db
from flask_login import LoginManager


web_dir = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'project_web')

app = Flask('PROJECT', root_path=web_dir,static_folder="static", static_url_path="")

login_manager = LoginManager()
login_manager.init_app(app)

if os.getenv('FLASK_ENV') == 'production':
    app.config.from_object("config.ProductionConfig")
else:
    app.config.from_object("config.DevelopmentConfig")



@app.before_request
def before_request():
    db.connect()


@app.teardown_request
def _db_close(exc):
    if not db.is_closed():
        print(db)
        print(db.close())
    return exc

@login_manager.user_loader
def load_user(user_id):
    return User_.get_or_none(id = user_id)     #to retrieve the current user