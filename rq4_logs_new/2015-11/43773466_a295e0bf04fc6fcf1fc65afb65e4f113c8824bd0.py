import json as simplejson
import requests

EMAIL = 'xxbb9000xx@gmail.com'
PASSWORD = 'stanley123'
PRODUCT_ID = '213940691'
CLIENT_KEY = 'dc5cd85c126243a6a9118602898aa074'

fileName = raw_input("Enter name of output file for reviews: ")

initialReq = requests.get('https://api.appfigures.com/v2/reviews?products={0}&count=500&end=2015-01-28&client_key={1}'.format(PRODUCT_ID, CLIENT_KEY), auth=(EMAIL, PASSWORD))

values = simplejson.loads(initialReq.text)
reviews = values['reviews']
months = ['02', '03', '04', '05', '06', '07', '08', '09', '10', '11']

for month in months:
    reqStr = 'https://api.appfigures.com/v2/reviews?products={0}&count=500&start=2015-{1}-01&end=2015-{1}-28&client_key={2}'.format(PRODUCT_ID, month, CLIENT_KEY)
    r = requests.get(reqStr, auth=(EMAIL, PASSWORD))
    reviews += simplejson.loads(r.text)['reviews']
    
with open(fileName, 'w') as outfile:
    outfile.write(simplejson.dumps(reviews, indent=4, sort_keys=True))
    outfile.close()
