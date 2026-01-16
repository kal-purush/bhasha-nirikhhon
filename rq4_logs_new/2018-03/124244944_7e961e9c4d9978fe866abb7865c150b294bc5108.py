from flask import Flask, request
import os
import socket
import numpy as np # linear algebr
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import pandas as pd
from random import randint
import json
import pyrebase

app = Flask(__name__)

config = {
  "apiKey": "AIzaSyD5BJ2gNDZUP9n7m1E7sYD2NjyKBLQgenU",
  "authDomain": "metro-data-mining-chatlog.firebaseapp.com",
  "databaseURL": "https://metro-data-mining-chatlog.firebaseio.com",
  "storageBucket": "metro-data-mining-chatlog.appspot.com"
}

firebase = pyrebase.initialize_app(config)
db = firebase.database()

# Get a reference to the auth service
auth = firebase.auth()

# Log the user in
user = auth.sign_in_with_email_and_password('metrodatamining@code.berlin', 'test123')

@app.route("/")
def hello():
	input = {}
	for param in inputparams:
		input[param] = request.args.get(param)
	
	return qs

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
