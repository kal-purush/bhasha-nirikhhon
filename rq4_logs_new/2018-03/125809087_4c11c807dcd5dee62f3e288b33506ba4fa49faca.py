# load config file

import json
import sys

__filename = "config.json"
__configJson = None

def get():
    try:
        global __configJson
        if __configJson != None:
            return __configJson
        json_data = open(__filename)
        __configJson = json.load(json_data)
        #print(__configJson)
        print("Config read from", __filename)
        return __configJson
    except:
        print("ERROR: could not load config from", __filename, sys.exc_info()[0])
        sys.exit()