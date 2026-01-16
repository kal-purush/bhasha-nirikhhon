#!/usr/bin/env 

import urllib2
import json

#Parameters :
symbol = 'ETH'
currency = 'EUR'

#API from cryptocopare.com
url = 'https://www.cryptocompare.com/api/data/price?fsym=' + symbol + '&tsyms='+ currency

json_obj = urllib2.urlopen(url)
data = json.load(json_obj)

for item in data['Data']:
	print item['Price']