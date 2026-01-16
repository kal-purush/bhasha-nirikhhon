from flask import Flask, jsonify, request, make_response
from flask_expects_json import expects_json
import argparse
import logging
import os
import sqlite3
import threading

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# globals
itemsDbConn = None
dbCursor = None
dbLock = threading.Lock()

@app.route('/')
def HelloWorld():
	return "hello, world"

GET_ITEM_INFO_SCHEMA = {
	"type": "object",
	"properties": {
		"item_name": {"type": "string"}
	},
	"required": ["item_name"]
}

VALIDATE_ITEMS_SCHEMA = {
	"$schema": "http://json-schema.org/draft-07/schema#",
	"items": {
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"quantity": {"type": "integer"}
		},
		"required": ["name", "quantity"]
	}
}

###########################################################################
##	
##	Returns the item ID, quantity, and price from the item name.
##	
###########################################################################
@app.route('/get_item_info', methods=['GET'])
@expects_json(GET_ITEM_INFO_SCHEMA)
def GetItemInfo():
	global itemsDbConn
	
	reqData = request.get_json()
	
	dbCursor.execute('SELECT id, price, quantity_in_stock FROM items WHERE product_name = ?', (reqData['item_name'],))
	item = dbCursor.fetchall()[0]
	
	return jsonify({'data': list(item)})

###########################################################################
##	
##	Validate items can all be purchased.  Returns 200 if yes, returns
##	400 if not.
##	
###########################################################################
@app.route('/validate_items', methods=['GET'])
@expects_json(VALIDATE_ITEMS_SCHEMA)
def ValidateItems():
	global itemsDbConn
	
	reqData = request.get_json()
	
	items = reqData['items']
	for item in items:
		itemName = item['item_name']
		quantity = item['quantity']
		
		dbCursor.execute('SELECT id, price, quantity_in_stock FROM items WHERE product_name = ?', (itemName,))
		dbItem = dbCursor.fetchall()[0]
		
		if dbItem[2] < quantity:
			return make_response(f'"{itemName}" not enough in stock', 500)
	
	return 'success'

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--db-directory', dest='db_directory', required=True)
	args = parser.parse_args()
	
	dbPath = os.path.join(args.db_directory, 'items.db')
	# check_same_thread = False means the write operations aren't thread safe, but we take care of that with global var dbLock
	itemsDbConn = sqlite3.connect(database=dbPath, check_same_thread=False)
	dbCursor = itemsDbConn.cursor()
	
	app.run(debug=True)