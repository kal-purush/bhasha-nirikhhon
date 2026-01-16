from flask import Flask, request

import rocksdb, uuid

import subprocess

import os,json


UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api/v1/scripts/')

app = Flask(__name__)


@app.route('/api/v1/scripts', methods=['POST'])

def post():

    db = rocksdb.DB("Test.db", rocksdb.Options(create_if_missing=True))

    file = request.files.get("data") # retrieving data from request.files dict
    
    filename=(file.filename)

    file.save(os.path.join(UPLOAD_FOLDER,filename))

    key = uuid.uuid4().hex

    db.put(key.encode(), (str(filename)).encode());

    resp = json.dumps({'script-id':key})

    return resp,201


@app.route('/api/v1/scripts/<scriptid>', methods=['GET'])

def get(scriptid):

    db = rocksdb.DB("Test.db", rocksdb.Options(create_if_missing=True))

    filename = db.get((scriptid.encode())).decode() #conversion for string and bytes data

    resp = subprocess.check_output(['python3.6', str(os.path.join(UPLOAD_FOLDER, filename))])

    return resp,200



if __name__ == '__main__':

    app.run('0.0.0.0',port=8000)