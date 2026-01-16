from flask import Flask, render_template, render_template_string, make_response, redirect
from flask.ext.restful import Api, Resource, reqparse, abort

import json
import string
import random
from datetime import datetime



# Assign URL paths to our resources.
app = Flask(__name__)
api = Api(app)

# Start the server.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888, debug=True)