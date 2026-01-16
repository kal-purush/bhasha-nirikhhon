from flask import Flask, url_for

from urllib.request import Request, urlopen
import json

from firebase import firebase

from time import localtime

app = Flask(__name__)
firebase = firebase.FirebaseApplication('https://hackcmu2016-a3c18.firebaseio.com/', None)


@app.route('/')
def index():
	return "Hello, World"

@app.route('/data')
def data():
	maplelist = pull_data(0)
	put_data(maplelist,0)
	result=firebase.get('/time',None)
	return result

def put_data(maplelist, worldID):
	time=localtime
	firebase.put('/time',str(time) + str(worldID), maplelist)


def pull_data(worldID):
	#pull request
	req = Request('http://maplestory.io/api/fm/world/0/rooms', headers={'User-Agent': 'Mozilla/5.0'})
	response = urlopen(req)

	#parsing
	string = response.read().decode('utf-8')
	maplelist = json.loads(string)
	print(maplelist[1]['shops'][0]['items'][0]) # prints the string with 'source_name' key

	return maplelist