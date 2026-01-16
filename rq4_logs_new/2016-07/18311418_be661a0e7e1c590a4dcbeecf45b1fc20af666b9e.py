#!/usr/bin/env Env_Python2711_Quandl289


# Draw chart of Energy consumption by use.
# https://www.quandl.com/collections/uk/uk-energy-data



import urllib
import sqlite3
import json
import time

serviceurl = urllib.urlopen("https://www.quandl.com/api/v3/datasets/EIA/IES_2_2_12_GBR.json?api_key=1bXF4N7MdecopWUkMvzn")
url = serviceurl.read()
json_data = json.loads(url)

print(type(json_data))
print(json_data)

count = 0
for item in json_data:
	count = count + 1
print count
