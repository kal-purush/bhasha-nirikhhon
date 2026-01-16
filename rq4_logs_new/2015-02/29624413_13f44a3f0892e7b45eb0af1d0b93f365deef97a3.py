__author__ = 'simranjitsingh'

import urllib
import urllib2
import json
import random
from time import sleep
import csv

url = 'http://205.188.201.176/geocoding/v1/address?key=Fmjtd%7Cluur2968ng%2Cr0%3Do5-90rs54&'

loc = "Harlem,Ny"
loc = "NYC"
purl = url + urllib.urlencode({"location": loc})
print purl
#r = requests.get(purl)
#rjson = json.loads(r.text)
request = urllib2.Request(purl)
res = urllib2.urlopen(request)
rjson = json.loads(res.read())
res.close()
print rjson
if len(rjson["results"][0]["locations"]) > 0:
    top_location = rjson["results"][0]["locations"][0]
    lat = top_location["latLng"]["lat"]
    lng = top_location["latLng"]["lng"]
    lat_lon = [lat,lng]
    print lat_lon
sleep(random.randrange(100,101) / 1000)