from urllib.request import Request, urlopen
from firebase import firebase
import json
from flask import Flask
from .forms import FirePut
from time import localtime

NUM_WORLDS=6

@app.route(‘/api/put’, methods=[‘GET’, ‘POST’])

def fireput(maplellist, worldID):
	form = FirePut()
	if form.validate_on_submit():
		putData = maplelist
		time = str(localtime())
		firebase.put('/time', time + str(worldID), putData)
		return render_template('api-put-result.html', form=form, putData=putData)
	return render_template('index.html')

def pull_data(worldID):
	#pull request
	req = Request('http://maplestory.io/api/fm/world/0/rooms', headers={'User-Agent': 'Mozilla/5.0'})
	response = urlopen(req)

	#parsing
	string = response.read().decode('utf-8')
	maplelist = json.loads(string)
	#print(maplelist[1]['shops'][0]['items'][0]['isIdentified']) # prints the string with 'source_name' key

	return maplelist

app = Flask(__name__)
firebase = firebase.FirebaseApplication(‘https://hackcmu2016-a3c18.firebaseio.com/’, None)

for i in range(NUM_WORLDS):
	fireput(pull_data(i),i)