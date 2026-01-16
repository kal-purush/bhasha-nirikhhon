"""Define Views."""
from flask import Flask, request, session, redirect, url_for, render_template, flash
import os
from models import User

app = Flask(__name__)

app.secret_key = os.environ.get('BS_SECRET_KEY')


@app.route('/')
def index():
    """Define index route."""
    return render_template('index.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Define registration route."""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if len(username) < 1:
            flash('Your username must be at least one character.')
        elif len(password) < 5:
            flash('Your password must be at least 5 characters.')
        elif not User(username).register(password):
            flash('A user with that username already exists.')
        else:
            session['username'] = username
            flash('Logged in.')
            return redirect(url_for('index'))

    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Manage login route."""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if not User(username).get():
            flash('Login failed.')
        elif not User(username).verify_password(password):
            flash('Login failed.')
        else:
            session['username'] = username
            flash('Logged in.')
            return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/profile', methods=['GET'])
def profile():
    """Manage profile route."""
    if not session.get('logged_in'):
        flash('please login to see your profile.')
        return redirect(url_for('login'))
    else:
        return render_template('profile.html')


@app.route('/logout', methods=['GET'])
def logout():
    """Manage logout route."""
    session.pop('logged_in', None)
    flash('You are logged out.')
    return redirect(url_for('login'))