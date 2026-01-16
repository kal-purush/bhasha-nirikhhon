from datetime import datetime, timedelta, timezone
from decimal import Decimal
from os import environ

import boto3
from boto3.dynamodb.conditions import Key
from flask import Flask, abort, jsonify
from flask.json import JSONEncoder
from flask_cors import CORS
from serverless_wsgi import handle_request


class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o == int(o):
                return int(o)
            return float(o)
        return super(DecimalEncoder, self).default(o)


app = Flask(__name__)
CORS(app)
app.json_encoder = DecimalEncoder
dynamodb = boto3.resource('dynamodb')


def query_all_events():
    table = dynamodb.Table(environ['EVENT_TABLE'])
    response = table.scan()
    events = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        events.extend(response['Items'])
    return events


def query_event_by_id(event_id):
    table = dynamodb.Table(environ['EVENT_TABLE'])
    response = table.get_item(Key={'event_id': event_id})
    if 'Item' not in response:
        raise ValueError(f'Event {event_id} not found')
    return response['Item']


def query_products_for_event(event_id):
    table = dynamodb.Table(environ['PRODUCT_TABLE'])
    key_expression = Key('event_id').eq(event_id)
    response = table.query(KeyConditionExpression=key_expression)
    products = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=key_expression,
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        products.extend(response['Items'])
    return products


def query_recent_products():
    table = dynamodb.Table(environ['PRODUCT_TABLE'])
    one_week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')
    key_expression = Key('status_code').eq('SUCCEEDED') & Key('processing_date').gte(one_week_ago)
    response = table.query(
        IndexName='processing_date',
        KeyConditionExpression=key_expression,
    )
    return response['Items']


@app.route('/events')
def get_events():
    events = query_all_events()
    return jsonify(events)


@app.route('/events/<event_id>')
def get_event_by_id(event_id):
    try:
        event = query_event_by_id(event_id)
    except ValueError:
        abort(404)
    event['products'] = query_products_for_event(event_id)
    return jsonify(event)


@app.route('/recent_products')
def get_recent_products():
    recent_products = query_recent_products()
    return jsonify(recent_products)


def lambda_handler(event, context):
    return handle_request(app, event, context)