from time import sleep
import requests
from requests.auth import HTTPBasicAuth
import pyrebase

STAT = {
    'API_KEY_USER': "uhaszhvk-qpo4-kiya",
    'API_KEY_PASS': "i9wd-qd093v3alhda",
    'EMAIL': 'bytecodedesigns@gmail.com',
    'PASSWORD': 'testpass1234',
    'ORDER_URL': "https://api.theprintful.com/orders",
    'ORDER_COUNT': 18,
    'EXP': 0,
    'SHIRT': 20,
    'TANK': 20,
    'SOCK': 10
}

FIREBASE_CONFIG = {
    'apiKey': "AIzaSyDDEhSlJDQK29tOZl57GVXM3rEE2ZypTSQ",
    'authDomain': "bytecode-designs.firebaseapp.com",
    'databaseURL': "https://bytecode-designs.firebaseio.com",
    'storageBucket': "bytecode-designs.appspot.com",
    "serviceAccount": "ByteCodeDesigns-f1bb2b287636.json"
}

def checkOrders():
    global STAT
    data = requests.get(STAT['ORDER_URL'], auth=HTTPBasicAuth(STAT['API_KEY_USER'], STAT['API_KEY_PASS']))
    formatted = data.json()['result']
    print(len(formatted))
    if STAT['ORDER_COUNT'] < len(formatted):
        unprocessed = len(formatted) - STAT['ORDER_COUNT']
        for x in formatted:
            if x['external_id']:
                processItems(x['items'])
            if unprocessed <= 0:
                updateFirebase()
                break
            unprocessed -= 1
    print(STAT['EXP'])

def processItems(items):
    global STAT
    for item in items:
        print(item['name'])
        if 'Shirt' in item['name']:
            print("This is a shirt")
            addExp(STAT['SHIRT'])
        elif 'Tank' in item['name']:
            print("This is a tank")
            addExp(STAT['TANK'])
        elif 'Sock' in item['name']:
            print("These are socks")
            addExp(STAT['SOCK'])
    STAT['ORDER_COUNT'] += 1

def addExp(addition):
    global STAT
    STAT['EXP'] += addition

def initFirebase():
    global STAT, USER
    FIREBASE.auth().refresh(USER['refreshToken'])
    db = FIREBASE.database()
    STAT['EXP'] = db.child("EXP").get(USER['idToken']).val()

def updateFirebase():
    global STAT, USER
    FIREBASE.auth().refresh(USER['refreshToken'])
    db = FIREBASE.database()
    db.update({'EXP': STAT['EXP']}, USER['idToken'])

FIREBASE = pyrebase.initialize_app(FIREBASE_CONFIG)
USER = FIREBASE.auth().sign_in_with_email_and_password(STAT['EMAIL'], STAT['PASSWORD'])
while True:
    initFirebase()
    checkOrders()
    sleep(10)