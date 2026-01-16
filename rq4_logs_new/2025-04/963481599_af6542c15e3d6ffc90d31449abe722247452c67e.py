from flask import Flask, jsonify
from threading import Event
import json
import signal
import os
import sys
import traceback

app = Flask(__name__)

INTERRUPT_EVENT = Event()

esi_api_host_name = os.environ.get('ESI_API_HOST_NAME') or ""
esi_api_port = int(os.environ.get('ESI_API_PORT') or "443")
esi_api_ssl = bool(os.environ.get('ESI_API_SSL') or "true")

@app.route('/api/v1/list/node', methods=['GET'])
def list_node():
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'}
    ]
    return jsonify(items)

@app.route('/api/v1/order/bare-metal', methods=['POST'])
def bare_metal_fulfillment_order_request():
    response = {
        'status': 'CREATED'
        , 'code': 201
    }
    return jsonify(response)

@app.route('/api/v1/list/networks', methods=['GET'])
def list_networks():
    items = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
        {'id': 3, 'name': 'Item 3'}
    ]
    return jsonify(items)

def start():
    flask_port = os.environ.get('FLASK_PORT') or 8081
    flask_port = int(flask_port)

    app.run(port=flask_port, host='0.0.0.0')

if __name__ == "__main__":
    start()