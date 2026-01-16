import os, sys
import requests
import time
import json
import urllib

import parameter.constants as constants
import parameter.filters as filters

from requests.auth import HTTPBasicAuth
from os.path import basename

id = sys.argv[1]
asset_type = 'analytic_sr'

session = requests.Session()
session.auth = (constants.API_KEY, '')

# first, activate the images
activate_item = session.get(constants.DOWNLOAD + "/" + constants.PLANET_4BAND + "/items/" + id + "/assets/")
activate_item_url = activate_item.json()[asset_type]["_links"]["activate"]
response_item = session.post(activate_item_url)

endpoint_search = {
  "item_types": [constants.PLANET_4BAND],
  "filter": filters.getFilterSetupById(id)
}

while(activate_item.json()[asset_type]['status'] == 'inactive'):
  print("Still inactive... lets wait a minute more!")
  time.sleep(60)

# second, download if the image is active
properties_item = session.post(constants.SEARCH, json=endpoint_search)
urllib.urlretrieve(activate_item.json()[asset_type]['location'], 'images/' + id + '.tif')
          
with open('images/' + item + '.json', 'w') as outf:                    
  json.dump(properties_item.json(), outf)

