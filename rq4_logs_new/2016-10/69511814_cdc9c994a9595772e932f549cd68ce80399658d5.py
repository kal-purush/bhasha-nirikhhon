import urllib.request, urllib.parse, urllib.error
import json

serviceurl = 'http://maps.googleapis.com/maps/api/geocode/json?'

address='San Jose, CA'
url = serviceurl + urllib.parse.urlencode({'address':address})
print('Receiving: ',url)
uh= urllib.request.urlopen(url)
data = uh.read().decode()
print('Received %s characters' %len(data))

try:
	js = json.loads(data)
except:
	js = None

if not js or 'status' not in js or js['status'] != 'OK':
	print('==== Failure to Retrieve ====')
	print(data)



print(json.dumps(js,indent=4))

#lat = js['results'][0]['geometry']['location']['lat']
#lng = js['results'][0]['geometry']['location']['lng']
#print('%s, %s' %(lat, lng))

addr_comp =js['results'][0]['address_components'][-1]
try:
	if 'country' in addr_comp['types']:
		code = addr_comp['short_name']
		print('Country code is: %s.' %code)
except:
	print('Country not applicable.')
