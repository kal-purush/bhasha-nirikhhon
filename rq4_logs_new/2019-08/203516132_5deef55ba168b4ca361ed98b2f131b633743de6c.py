from flask import Flask, render_template,request,redirect,url_for # For flask implementation
from bson import ObjectId # For ObjectId to work
from pymongo import MongoClient
import os
import json
app = Flask(__name__)
title = "Azure IoT POC Server"
heading = "AzureIoT Hub backend"

##Un-Comment when running against the Cosmos DB Emulator
client = MongoClient("mongodb://rpi:FFwCOdmAy55XI2FqKDQX0qoQUeAweYMegGofQnCcBseVVKMhIE2UbzyJMTMbVOYTncaRa5HffqQTlscm20GHdA==@rpi.documents.azure.com:10255/?ssl=true&replicaSet=globaldb") #host uri
db = client.iot    #Select the database
db.authenticate(name="rpi",password='FFwCOdmAy55XI2FqKDQX0qoQUeAweYMegGofQnCcBseVVKMhIE2UbzyJMTMbVOYTncaRa5HffqQTlscm20GHdA==')

## Comment out when running locally
# client = MongoClient(os.getenv("mongodb://rpi:FFwCOdmAy55XI2FqKDQX0qoQUeAweYMegGofQnCcBseVVKMhIE2UbzyJMTMbVOYTncaRa5HffqQTlscm20GHdA==@rpi.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"))
# db = client.iot    #Select the database
# db.authenticate(name=os.getenv("rpi"),password=os.getenv("FFwCOdmAy55XI2FqKDQX0qoQUeAweYMegGofQnCcBseVVKMhIE2UbzyJMTMbVOYTncaRa5HffqQTlscm20GHdA=="))
msg = db.messages #Select the collection

def redirect_url():
    return request.args.get('next') or \
           request.referrer or \
           url_for('index')

@app.route("/getTemperatureHumidity")
def dht11 ():
	#Display the all Tasks
	li = []
	for doc in msg.find():
		li.append(doc)
	
	resDict = li[len(li)-1]
	resDictFinal = {'temperature' : resDict['temperature'], 'humidity' : resDict['humidity']}
	response = app.response_class(
        response= json.dumps(resDictFinal),
        status=200,
        mimetype='application/json'
		)
	return response
@app.route("/getPushBtnCount")
def pushBtn ():
	#Display the all Tasks
	li = []
	for doc in msg.find():
		li.append(doc)
		print(type(doc))
	
	resDict = li[len(li)-1]
	resDictFinal = {'count' : resDict['count']}
	response = app.response_class(
        response= json.dumps(resDictFinal),
        status=200,
        mimetype='application/json'
		)
	return response


# define for IIS module registration.

wsgi_app = app.wsgi_app

if __name__ == "__main__":
	app.debug = True
	app.run()



