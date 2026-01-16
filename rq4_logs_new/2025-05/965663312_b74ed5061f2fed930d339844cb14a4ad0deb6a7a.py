from flask import Flask, jsonify, request, make_response
from flask_expects_json import expects_json
import argparse
import json
import logging
import os
import requests
import sqlite3
import threading

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# "constants"
JSON_HEADER_DATATYPE = {'Content-type': 'application/json'}

# json schemas
QUEUE_ADD_DEL_ITEM = {
	"type": "object",
	"properties": {
		"user_id": {"type": "integer"}, # @todo swelter: change this to user email and query for user id in call
		"item_id": {"type": "integer"}, # @todo swelter: change this to item name and make shopping cart service query for item id
		"quantity": {"type": "integer"}
	},
	"required": ["user_id", "item_id", "quantity"]
}

GET_PURCHASE_QUEUED_ITEMS = {
	"type": "object",
	"properties": {
		"user_id": {"type": "integer"} # @todo swelter: change this to user email and query for user id in call
	},
	"required": ["user_id"]
}

@app.route('/')
def HelloWorld():
	return "hello, world"

###########################################################################
##	
##	Queues the specific item
##	
###########################################################################
@app.route('/queue_item', methods=['POST'])
@expects_json(QUEUE_ADD_DEL_ITEM)
def QueueAddItem():
	# global ordersDbConn
	# global dbCursor
	
	reqData = request.get_json()
	
	# Simply ensure a cart exists
	# @todo swelter: change this to something more permanent later
	url = 'http://127.0.0.1:2000/get_or_create_cart'
	postData = {'user_id': reqData['user_id']}
	resp = requests.post(url=url, data=json.dumps(postData), headers=JSON_HEADER_DATATYPE)
	
	# if something failed here, it's quite bad, just return error
	if resp.status_code != 200:
		return make_response(resp.text, resp.status_code)
	
	# try:
	# 	respJson = resp.json()
	# except requests.exceptions.JSONDecodeError:
	# 	return make_response('failed to decode json getting cart id', 500)
	
	url = 'http://127.0.0.1:2000/add_item_to_cart'
	postData = {'user_id': reqData['user_id'], 'item_id': reqData['item_id'], 'quantity': reqData['quantity']}
	resp = requests.post(url=url, data=json.dumps(postData), headers=JSON_HEADER_DATATYPE)
	
	if resp.status_code != 200:
		return make_response(resp.text, 500)
	
	return 'success'

###########################################################################
##	
##	Gets current item queue
##	
###########################################################################
@app.route('/get_queued_items', methods=['GET'])
@expects_json(GET_PURCHASE_QUEUED_ITEMS)
def GetQueuedItems():
	reqData = request.get_json()
	
	url = 'http://127.0.0.1:2000/get_cart_items'
	getData = {'user_id': reqData['user_id']}
	resp = requests.get(url=url, data=json.dumps(getData), headers=JSON_HEADER_DATATYPE)
	
	# if no cart found, return empty item list
	if resp.status_code == 500:
		if resp.text == 'no_cart':
			return jsonify({'items': []})
		else:
			return make_response('error getting items in cart', 500)
	
	try:
		respJson = resp.json()
	except requests.exceptions.JSONDecodeError:
		return make_response('failed to decode json getting cart items', 500)
	
	return jsonify(respJson)

###########################################################################
##	
##	Purchase current queue of items
##	
###########################################################################
@app.route('/purchase_queue', methods=['POST'])
@expects_json(GET_PURCHASE_QUEUED_ITEMS)
def PurchaseQueuedItems():
	reqData = request.get_json()
	
	url = 'http://127.0.0.1:2000/purchase_cart'
	postData = {'user_id': reqData['user_id']}
	resp = requests.post(url=url, data=json.dumps(postData), headers=JSON_HEADER_DATATYPE)
	
	# if there was an error purchasing cart, return it
	if resp.status_code == 500:
		return make_response(resp.text, 500)
	
	try:
		respJson = resp.json()
	except requests.exceptions.JSONDecodeError:
		return make_response('failed to decode json purchasing cart items', 500)
	
	# return purchased items
	return jsonify(respJson)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--db-directory', dest='db_directory', required=True)
	args = parser.parse_args()
	
	app.run(debug=True)