from flask import Flask
import json
import logging
from datetime import date, datetime
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

app = Flask(__name__)

@app.route("/sensors/environment")
def environment():
    mongoc = MongoClient('localhost', username='www', password='webmongo', authSource='sensors')
    mongo_sensors = mongoc.sensors
    data = [d for d in mongo_sensors.environment.find(projection={'_id':False})]
    resp = json.dumps(data, default=json_serial)
    return (resp, 200, {'Content-type': 'application/json'})

@app.route("/sensors/environment/limit/<int:records>")
def environment_limit(records):
    mongoc = MongoClient('localhost', username='www', password='webmongo', authSource='sensors')
    mongo_sensors = mongoc.sensors
    data = [d for d in mongo_sensors.environment.find(sort=[('_id', -1)], limit=records, projection={'_id':False})]
    resp = json.dumps(data, default=json_serial)
    return (resp, 200, {'Content-type': 'application/json'})


if __name__ == "__main__":
    app.run(debug=True)