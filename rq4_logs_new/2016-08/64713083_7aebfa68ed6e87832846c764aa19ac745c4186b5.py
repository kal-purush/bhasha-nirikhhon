from flask import Flask, render_template
from flask_login import LoginManager
from flask_login import login_required
from flask_login import login_user
from flask import redirect
from flask import url_for
from flask import request
from dbhelp import dbhelp as DBHelper
from user import User


DB = DBHelper()

app = Flask(__name__)
login_manager = LoginManager(app)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/account")
#@login_required
def account():
    return "Welcome, you are here!"

@app.route("/login", methods=["POST"])
def login_user(user):
    email = request.form.get("email")
    password = request.form .get("password")
    user_password = DB.get_user(email)
    if user_password and user_password == password:
        user = User(email)
        login_user(user, remember=True)
        return redirect(url_for('account'))
    return home()

@login_manager.user_loader
def load_user(user_id):
    user_password = DB.get_user(user_id)
    if user_password:
        return User(user_id)


app.secret_key = '0ZyLk/0K1+GVdZTRWCfK441+CUHGmRKXzly1UXnws29VOYHuVTCyhFfMh4c9OcWUgL8iGgk5a99t9T1Xo5EsC9wmoTAiDPZu8US'

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')