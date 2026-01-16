from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash

from .models import User

bp = Blueprint('auth', __name__)

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        error = None

        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            session.clear()
            session['user_id'] = user.id

            return redirect(url_for('index'))
        else:
            error = 'Invalid username or password'

        flash(error)

    if g.user:
        return redirect(url_for('index'))

    return render_template('auth/login.html')

@bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        confirm_password = request.form['confirm-password']
        error = None

        if not username:
            error = 'Username is required'
        elif not password or not confirm_password:
            error = 'Password is required'
        elif password != confirm_password:
            error = 'Password and Confirm Password don\'t match'
        else:
            existing_user = User.query.filter_by(username=username).first()
            # Don't enumerate usernames
            if existing_user is None:
                new_user = User(username=username, password=generate_password_hash(password))
                db.session.add(new_user)
                db.session.commit()

            return redirect(url_for('auth.login'))

        flash(error)

    if g.user:
        return redirect(url_for('index'))

    return render_template('auth/register.html')

@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))