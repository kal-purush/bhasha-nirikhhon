import os
from flask import Flask, redirect
import click
import site_service


app = Flask(__name__, instance_relative_config=True)
app.config.from_mapping(SECRET_KEY='dev')


@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


@app.route('/')
def home():
    return redirect('/Home')


app.register_blueprint(site_service.bp)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

