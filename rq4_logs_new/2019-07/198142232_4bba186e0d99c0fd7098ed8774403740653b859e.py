from flask import Flask, render_template
app = Flask(__name__)

import urllib.request
import json
import webbrowser

apiurl = 'https://api.nasa.gov/planetary/apod?'
key = 'api_key=km0oHfbwaxRqLn5H71N8szjF4593vawxBF5vhpSn'


def callAPI(apiurl, key):
	return urllib.request.urlopen(apiurl + key)

def readAPI(callapi):
	return callapi.read()

def decodeAPI(readapi):
	return json.loads(readapi.decode('utf-8'))

api = decodeAPI(readAPI(callAPI(apiurl, key)))

@app.route('/')
def index():
 	return render_template('index.html', api=api)