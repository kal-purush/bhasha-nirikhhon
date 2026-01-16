#!/usr/bin/env python3

# This is our controller.

import os
import random
import time

from flask import Flask, g, redirect, render_template, request, session, url_for

from application.extend import authorize
from application.models import Customer
from application.views  import module as views
import application.views as view


def keymaker(omnibus, filename='secret_key'):
    pathname = os.path.join(omnibus.instance_path, filename)
    try:
        omnibus.config['SECRET_KEY'] = open(pathname, "rb").read()
    except IOError:
        parent_directory = os.path.dirname(pathname)
        if not os.path.isdir(parent_directory):
            os.system('mkdir -p {0}'.format(parent_directory))
        os.system('head -c 24 /dev/urandom > {0}'.format(pathname))

omnibus = Flask(__name__)

omnibus.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///sql.db"
omnibus.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

omnibus.register_blueprint(views)

keymaker(omnibus)
customer = Customer()
# customer.setup(omnibus)


@omnibus.before_request
def before_request():
    g.customer = None
    if 'customer_id' in session:
        # FIXME TO BE CONTINUED...
        print(session['customer_id'])
        g.customer = customer.get_customer(session['customer_id'])


# Front page
@omnibus.route('/', methods=['GET'])
def front():
    return render_template('index.html')


# FIXME Login page
@omnibus.route('/login', methods=['GET', 'POST'])
def login():
    # # Check if the username and password were submitted
    #     # Check if the username exists
    #         # Check if the submitted password is the stored password for the username
    # return render_template('login.html')

    if request.method == 'GET':
        return render_template('login.html')
    else:
        fuzz             = random.uniform(1,9999) / 100000
        username         = request.form['username']
        password         = request.form['password']
        # Check if the username and password were submitted
        if username and password:
            # Check if the passwords match
            time.sleep(fuzz)
            # Check if the username exists (in the database)
            # TODO Re-factor the following into an ORM for PostgreSQL
            import sqlite3
            # --> Open up a connection and cursor objects
            connection = sqlite3.connect('run/datastore/sql.db', check_same_thread=False)
            cursor     = connection.cursor()
            # --> Execute a query
            cursor.execute('SELECT COUNT(*), MIN(id) FROM users WHERE username="{0}" AND password="{1}";'.format(username, password))
            (tally, customer_id) = cursor.fetchall()[0]
            if tally < 1:
                # bad creds
                pass
            elif tally > 1:
                # corrupt database
                pass
            else:
                # good creds
                # FIXME Assign objects to `g` and `session`
                pass

            if tally == 1:
                time.sleep(fuzz)
                # State: Username doesn't exist
                # Store submitted username and password in the database
                # TODO Mitigate a hypothetical SQL injection attack by
                #      replacing the format invocation with something else.
                # cursor.execute('INSERT INTO users(username, password) VALUES("{0}","{1}");'.format(username, password))
                # connection.commit()
                # cursor.close()
                # connection.close()
                # return redirect(url_for('views.dashboard'))
            else:
                # State: Username exists
                # FIXME
                pass
            # Close the aforementioned connection and cursor objects
            # FIXME
            # close this
            # close that
        else:
            # FIXME
            pass


# FIXME Register page
@omnibus.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return render_template('register.html')
    else:
        username         = request.form['username']
        password         = request.form['password']
        password_confirm = request.form['password_confirm']
        # Check if the username and passwords were submitted
        if username and password and password_confirm:
            # Check if the passwords match
            if password == password_confirm:
                # TODO Modularize the following PRNG.
                # Note: This is an attempt to mitigate a timing attack, by Eve,
                #       which would hypothetically calculate the Levenshtien
                #       distance between the submitted password and the
                #       submitted username for an existing user.
                fuzz = random.uniform(1,9999) / 100000
                time.sleep(fuzz)
                customer_id = customer.register(username, password)
                session['logged_in'] = True
                session['customer_id'] = customer_id
                return redirect(url_for('views.dashboard')) # FIXME
            else:
                # FIXME
                pass
        else:
            # FIXME
            pass


# TODO 404 page
@omnibus.route('/404', methods=['GET'])
def error_404():
    return render_template('404.html')