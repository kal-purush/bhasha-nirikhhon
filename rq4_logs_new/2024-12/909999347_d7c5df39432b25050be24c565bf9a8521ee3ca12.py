#!/usr/bin/env python
import requests,json
import hashlib
import datetime
from datetime import date, datetime,timezone,timedelta
from requests.models import HTTPError
from zoneinfo import ZoneInfo

import check_free_electricity

DEBUG = False
CONFIG_FILE = "config.txt"
tessieapikey="XXXXXXXXXXXXXXXXXXXXXXXXXXX"

try:
   f = open(CONFIG_FILE,"r")
except:
   print("Config file ("+CONFIG_FILE+") does not exist. Please execute IO-Update-Powerwall-Schedule first to create a blank config file\nThen add the Tessia API Key and rerun this script\n")
   quit()

for line in f:
  if(line!="\n" and line.strip()!="" and line.strip()[0]!="#"):
    linesplit = line.strip().split(" ")
    if(linesplit[0] == "TESSIE_API_KEY"):
      tessieapikey = linesplit[1]

if(tessieapikey=="XXXXXXXXXXXXXXXXXXXXXXXXXXX"):
  print("It looks like you've not edited your Tessie API key in the config file. Please do that and rerun the script.\nQuitting...")
  quit()

# Tesla URL to get the Site ID
teslaurl = "https://api.tessie.com/api/1/products"

def getStatus(tessieapikey, teslaurl):
    try:
        url=teslaurl
        headers={"Content-Type": "application/json","Authorization": "Bearer "+tessieapikey}
        if DEBUG:
           print("Get Status Headers: "+str(headers))
           print("Get Status URL: "+url)
        r = requests.get(url,headers=headers)
        if DEBUG:
          print(f'Get Status HTTP Error {r.status_code}') 
          print(f'Get status HTTP Message {r.reason}')
        if DEBUG:
           print("Get Status Result: \n"+r.text)
           print("\n-------------------------------------------------------------------\n")
        print("Tesla Site ID - add this to your config.txt: "+str(json.loads(r.text)["response"][1]["energy_site_id"]))

    except HTTPError as http_err:
        print(f'Status: HTTP Error {http_err}')
    except Exception as err:
        print(f'Status: Another error occurred: {err}')

getStatus(tessieapikey,teslaurl)