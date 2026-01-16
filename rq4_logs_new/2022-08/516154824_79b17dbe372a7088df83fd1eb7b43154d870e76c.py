import time
import requests
import os
from dotenv import load_dotenv
import random

load_dotenv()

# TOKEN = "..."  # Put your TOKEN here
TOKEN = os.getenv('TOKEN') # Put your TOKEN here
DEVICE_LABEL = os.getenv('DEVICE_LABEL')  # Put your device label here 
VARIABLE_LABEL_1 = os.getenv('VARIABLE_LABEL_1')  
VARIABLE_LABEL_2 = os.getenv('VARIABLE_LABEL_2')
VARIABLE_LABEL_3 = os.getenv('VARIABLE_LABEL_3')
VARIABLE_LABEL_4 = os.getenv('VARIABLE_LABEL_4')
VARIABLE_LABEL_5 = os.getenv('VARIABLE_LABEL_5')

VARIABLE_ID = os.getenv('VARIABLE_ID')  # Put your first variable label here


def build_payload(variable_1, variable_2, variable_3, variable_4, variable_5, 
                    value_1, value_2, value_3, value_4, value_5):

    payload = {variable_1: value_1, variable_2: {'value': 2, 'context':{'name': value_2}}, variable_3: value_3, variable_4: {'value': 4, 'context':{'name': value_4}}, variable_5: value_5}

    return payload

def post_request(payload):
    # Creates the headers for the HTTP requests
    try:
        url = "http://industrial.api.ubidots.com/api/v1.6/devices/"+ DEVICE_LABEL +"/" 
        headers = {"X-Auth-Token": TOKEN, "Content-Type": "application/json"}

        # Makes the HTTP requests
        req = requests.post(url=url, headers=headers, json=payload)
        status = req.status_code
        time.sleep(1)

        # Processes results
        print(req.status_code, req.json())
        if status >= 400:
            print("[ERROR] Could not send data after 5 attempts, please check \
                your token credentials and internet connection")
            pass

        print("[INFO] request made properly, your device is updated")
        return True
    except:
        print("Cannot send to ubidots")
        pass

def randomData():
    value_1 = random.randint(60,100)
    value_2 = 'Nauval'
    value_3 = random.randint(90,100)
    value_4 = 'Normal'
    value_5 = random.randint(37,40)

    return value_1, value_2, value_3, value_4, value_5


def sendData(value_1, value_2, value_3, value_4, value_5):

    payload = build_payload(
        VARIABLE_LABEL_1, VARIABLE_LABEL_2, VARIABLE_LABEL_3, VARIABLE_LABEL_4, VARIABLE_LABEL_5,
        value_1, value_2, value_3, value_4, value_5)

    print("[INFO] Attemping to send data")
    print(payload)
    post_request(payload)
    print("[INFO] finished")


if __name__ == '__main__':
    while (True):
        val = randomData()

        sendData(val[0], val[1], val[2], val[3], val[4])
        time.sleep(1)