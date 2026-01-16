import os
import datetime
import time
from ConfigParser import SafeConfigParser
import sys
import uuid
import ibmiotf.device
import requests
import json
from requests_toolbelt.multipart import encoder

#Bluemix IoT related variables
organization = None
deviceType = None
deviceId = None
authMethod = None
authToken = None
apiKey = None
deviceCli = None

def setConfigVariables():
    global organization, deviceType, deviceId, authMethod, authToken, apiKey
    parser = SafeConfigParser()
    parser.read('config.ini')

    #load Bluemix config data
    organization = parser.get('bluemix_config','organization')
    deviceType = parser.get('bluemix_config', 'device_type')
    deviceId = parser.get('bluemix_config', 'device_id')
    authMethod = parser.get('bluemix_config', 'auth_method')
    authToken = parser.get('bluemix_config', 'auth_token')
    apiKey = parser.get('bluemix_config', 'api_key')


def imgStoreMonitor_callback(monitor):
    # Do something with this information
    #print 'bytes_read=' + len(monitor)
    pass




def setupIOTConnection () :

    #make sure the variables are setup.
    setConfigVariables()

    #get access to the global variables.
    global organization, deviceType, deviceId, authMethod, authToken, apiKey, deviceCli
    
    # Initialize the device client for Bluemix IoT
    try:
        deviceOptions = {"org": organization,
        "type": deviceType,
        "id": deviceId,
        "auth-method": authMethod,
        "auth-token": authToken}
        deviceCli = ibmiotf.device.Client(deviceOptions)
    except Exception as e:
        print("Caught exception connecting device: %s" % str(e))
        sys.exit()


def connectIOT():
    global deviceCli
    # Connect to Bluemix IoT
    deviceCli.connect()



def notifyIOT(data) :

    def myOnPublishCallback():
        print("Confirmed event %s received by IoTF\n" % data)

    #send data to IoT
    success = deviceCli.publishEvent("status", "json", data, qos=0, on_publish=myOnPublishCallback)

    if not success:
        print("Not connected to IoTF")

