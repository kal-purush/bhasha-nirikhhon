#!/usr/bin/python
'''preamble: 
1) read input file (semi-colon delimited list of fields)
2) format data
3) and submit to Sync API endpoint 

reference: POST /api/v1/<Publisher_API_KEY>/sync_customers/ HTTP/1.1
https://doc.agent.ai/developer/integrations/#customer-information-sync-api
'''

import sys
import json
import requests
import base64

# CONSTANTS...SETTABLE OPTIONS FOR DIFFERENT CONFIGURATIONS
API_KEY = 'change_me' #prod
SYNC_SERVER = 'my.agent.ai' #prod
PUBLISHER_ADMIN_USERNAME = 'admin@company.com'
PUBLISHER_ADMIN_PASSWORD = 'changeme'

input_file = open(sys.argv[1], 'r')
toss = input_file.readline() #toss header
keys = ['id','last_name']
customers = []
url = "https://" + SYNC_SERVER + "/api/v1/" + API_KEY + "/sync_customers/"
# The authentication header for the web service
HTTP_BASIC_AUTHORIZATION = base64.b64encode(PUBLISHER_ADMIN_USERNAME + ':' + PUBLISHER_ADMIN_PASSWORD)

for line in input_file:
	line = line.rstrip()
	line = line.decode('latin-1') #spanish,italian,brazilian,mexican
	cust_list = line.split(";")
	cust_obj = dict(zip(keys, cust_list)) #gets 2 of 3
	blk = {"blacklisted":cust_list[-1]} #grabs last
	cust_obj["properties"] = blk
	customers.append(cust_obj)

all_customers = {"customers":customers}
all_customers_json = json.dumps(all_customers)
#print all_customers_json
input_file.close()
#sys.exit(0)
headers = {
	'content-type': "application/json",
	'cache-control': "no-cache",
	'authorization': "Basic " + HTTP_BASIC_AUTHORIZATION
	}

response = requests.request("POST", url, data=all_customers_json, headers=headers)

print(response.text)