from flask import Flask, url_for

from urllib.request import Request, urlopen
import json

from firebase import firebase

from time import localtime, clock
from copy import deepcopy

NUM_WORLDS=6

app = Flask(__name__)
firebase = firebase.FirebaseApplication('https://maple-6845a.firebaseio.com/', None)

for i in range(NUM_WORLDS):
	firebase.put('/worldID',str(i)+'transactions', [-1])

@app.route('/')
def index():
	#firebase.delete('/time',None) #kill the database
	return 'Hello, World'

@app.route('/data')
def data():
	temp=clock()
	for i in range(NUM_WORLDS):
		maplelist = pull_data(i)
		current=firebase.get('/worldID',str(i))
		transactions(current,maplelist,i)
		put_data(maplelist,i)
	return(str(temp-clock())+' seconds')

def indexSameShop(shopname, thelist):
	for x in range(len(thelist)):
		if thelist[x]['shopName'] == shopname:
			return x
	return -1



def transactions(old, new, worldID):
	assert len(old)==len(new)
	result=firebase.get('/worldID',str(worldID)+'transactions')
	for i in range(len(old)):
		assert old[i]['channel']==new[i]['channel'], "not same channel"
		assert old[i]['room']==new[i]['room'], "not same room"

		#NEW SHOPS
		#CLOSED SHOPS
		#CHANGED NAME SHOPS

		if 'shops' in old[i].keys():
			for j in range(len(old[i]['shops'])):
				#IF NAME OF SHOP WAS CHANGED
				x = indexSameShop(old[i]['shops'][j]['shopName'], new[i]['shops'])
				if(x==-1):
					pass
				
				if 'items' in old[i]['shops'][j].keys():
					for item in	old[i]['shops'][j]['items']:
						
						if 'items' in new[i]['shops'][x].keys():
							for thing in new[i]['shops'][x]['items']:

								if item['name']==thing['name'] and item['numberOfEnhancements']==thing['numberOfEnhancements'] and item['quantity']>thing['quantity']:
									result.append(deepcopy(item))
									result[-1]['quantity']=item['quantity']-thing['quantity']
									result[-1]['time']=localtime()

	firebase.put('/worldID',str(worldID)+'transactions', result)

def put_data(maplelist, worldID):
	firebase.put('/worldID',str(worldID), maplelist)


def pull_data(worldID):
	#pull request
	path='http://maplestory.io/api/fm/world/'+str(worldID)+'/rooms'
	req = Request(path, headers={'User-Agent': 'Mozilla/5.0'})
	response = urlopen(req)

	#parsing
	string = response.read().decode('utf-8')
	maplelist = json.loads(string)
	#print(maplelist[1]['shops'][0]['items'][0]) # prints the string with 'source_name' key

	return maplelist