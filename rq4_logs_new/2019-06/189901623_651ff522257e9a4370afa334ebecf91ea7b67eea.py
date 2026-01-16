#!/usr/bin/env python3
#  user class

from flask import Flask, render_template, jsonify, redirect, url_for, request
from models import storage
import models
import json

app = Flask(__name__)
app.url_map.strict_slashes = False
host = '127.0.0.1'
host = '192.168.33.10'

def page_not_found(e):
    """ 404 error page for nonexistent routes """
    return jsonify({'error': "Not found"}), 404


@app.route("/", methods=['GET', 'POST'])
def login():
    """ render login homepage """
    error = None
    if request.method == 'POST':
        uname = request.form.get('email')
        objs = storage.query("User")
        for obj in objs:
            if uname in obj.__dict__.values():
                """
                REDIRECT HERE!
                """
                print("OBJ TYPE {}".format(obj))

                my_dict = obj.to_dict()

                jsoned = json.dumps(my_dict)

                return redirect(url_for('.landing', jsoned=jsoned))

        error = 'Invalid Username or Password. Please try again'
    return render_template('index.html')

@app.route("/landing")
def landing():
    obj_dict = json.loads(request.args['jsoned'])
    """
    re-create obj
    """
    return render_template('landing.html')

@app.route('/lend', methods=['GET'])
def lend():
    return '<h1> Lend, Here!</h1>'

if __name__ == "__main__":
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    app.register_error_handler(404, page_not_found)
    app.run(host=host, port=5000, debug=True)