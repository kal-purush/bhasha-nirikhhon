#!/usr/bin/env python
import sys
import os
import urllib
import urllib2
import json
from twython import Twython
from nltk.stem.porter import *

#Twitter stuff  
CONSUMER_KEY = '1S5jKHnMuoH1GiRY5LbPKie6L'
CONSUMER_SECRET = 'W7QJNgFHdnGBiMuTVlgh1ZhOe6sbKWiacudxDiiCim3SPhd8Gn'
ACCESS_KEY = '2956164869-ohrgiTpiMBTqiI43kqZ2iFr274efzJszHPpzcD5'
ACCESS_SECRET = 'LV6N6Rnf6E1fVXXMZFehNRUUohxZgi5k6bTOQ2RVpE3nE'
api = Twython(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET)

#Random movie generator
movieUrl = 'http://www.suggestmemovie.com'
movieResponse = urllib2.urlopen(movieUrl)
moviePage = movieResponse.read()
movieIndex = moviePage.find('MOVIES SUGGESTED:')
find1 = moviePage[movieIndex:]
movieIndex = find1.find('title')
find2 = find1[movieIndex+7:]
secondIndex = find2.find(' (')
movie = find2[:secondIndex]

#Plot finder
plotUrl = 'http://www.omdbapi.com/'
values = { 't' : movie, 'plot': 'short', 'r': 'json'}

data = urllib.urlencode(values)
response = urllib.urlopen(plotUrl + '?%s' % data)
the_page = response.read()
page_converted = json.loads(the_page)

plot = page_converted['Plot']
plot = plot[:120]
plot.rsplit(' ', 1)[0]
plot = plot+"..."

#Plot editor
#for word in plot.split(""):
  
#"".join(PorterStemmer().stem_word(word) for word in text.split('')

print(''+movie+', '+plot)

api.update_status(status=''+movie+' '+plot)