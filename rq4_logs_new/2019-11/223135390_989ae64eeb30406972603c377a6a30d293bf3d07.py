from flask import Blueprint, render_template,request,redirect,url_for,flash
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename
from flask_login import login_required,login_user,logout_user,current_user

surveys_blueprint = Blueprint('surveys',
                            __name__,
                            template_folder='templates')


@surveys_blueprint.route('/new', methods=['GET'])
def new():
    return render_template('surveys/new.html')

@surveys_blueprint.route('/create', methods=['GET'])
def create():
    return render_template('surveys/new.html')