from flask import Flask, request
from datetime import datetime
from google.cloud import datastore

app = Flask(__name__)
db = datastore.Client()

def store_data(gid, nid, sensor):
    entity = datastore.Entity(key=db.key('iot'))
    entity.update({
        'gid': gid,
        'nid': nid,
        'sensor': sensor,
        'timestamp': datetime.now()
    })
    db.put(entity)


def fetch_data():
    query = db.query(kind='iot')
    #query.add_filter('sensor','>=', 450)
    datas = query.fetch()

    return datas

@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return 'Welcome to IoT in SCU'

@app.route('/store')
def store():
    gid = request.args.get('gid')
    nid = int(request.args.get('nid'))
    sensor = int(request.args.get('sensor'))
    store_data(gid, nid, sensor)
    return 'OK'

@app.route('/fetch')
def fetch():
    datas = fetch_data()
    out = ''
    for data in datas:
        out = out + '{}:{}:{}:{}<br>'.format(data['gid'],data['nid'],data['sensor'],data['timestamp'])
    return out

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)