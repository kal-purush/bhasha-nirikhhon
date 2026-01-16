import RPi.GPIO as GPIO
import time
import datetime
import pyrebase
GPIO.setmode(GPIO.BCM)

DOOR_SENSOR_PIN = 18

isOpen = None
oldIsOpen = None

GPIO.setup(DOOR_SENSOR_PIN, GPIO.IN, pull_up_down =  GPIO.PUD_UP)



config = {
  "apiKey": "AIzaSyDcEeWJw_U7T_Nm7uto-J5GCz9jHeEPJd0",
  "authDomain": "supportbox-dd807.firebaseapp.com",
  "databaseURL": "https://supportbox-dd807.firebaseio.com",
  "projectId": "supportbox-dd807",
  "storageBucket": "supportbox-dd807.appspot.com",
}

firebase = pyrebase.initialize_app(config)
dataOpen = {"status": "1"}
dataClosed = {"status": "0"}
data = {"Ident": "box1", "message":"box1 opened"}
firebase = pyrebase.initialize_app(config)
db = firebase.database();
user = {"name": "Tito Bandito",
        "_id": "1"
}

message = {
    "text": "Anonymous user's box has opened!",
     "user": user,
     "timestamp": time.time()
    }

while True:
    
    oldIsOpen = isOpen
    isOpen = GPIO.input(DOOR_SENSOR_PIN)
    if (isOpen and (isOpen != oldIsOpen)):
        keynum = db.child("Box").child(1).child("group_key").get()
        if (keynum.key() == 0):
            break
        else:
            db.child("Messages").child("1").push(message)
            print ("Sending open signal")
    elif (isOpen != oldIsOpen):
        print ("CLOSED")
        
time.sleep(0.1)  