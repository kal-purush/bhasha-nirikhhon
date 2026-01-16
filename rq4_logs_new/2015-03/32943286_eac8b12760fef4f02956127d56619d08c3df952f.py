import urllib   # urlencode function
import urllib2  # urlopen function (better than urllib version)
import json

WUNDERGROUND_API_KEY = "3ec21efc1c37b8b2"
WUNDERGROUND_BASE_URL = "http://api.wunderground.com/api/"+WUNDERGROUND_API_KEY


def json(url):
	f = f = urllib2.urlopen(url)
	return f.read()


def weather_on(date):
	return json(WUNDERGROUND_BASE_URL+ "/history_" + date + "/q/MA/Boston.json")

print weather_on('20150328')