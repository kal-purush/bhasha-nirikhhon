from flask import *
import mysql.connector
import json

app = Flask(__name__)

@app.route('/login', methods=['GET'])
def login():
    return render_template("login.php")