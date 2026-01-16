import difflib
import requests
import json
from collections import namedtuple

#converts json into object
def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())
def json2obj(data): return json.loads(data, object_hook=_json_object_hook)

#true if the word is included in the list of supplied words
def hasWord(str, goodWords):
	return len(difflib.get_close_matches(str, goodWords)) > 0

#find news outlet based on input
def findOutlet(str):
	str = str.lower()
	words = str.split()
	words = [x for x in words if x != "news"]
	for word in words:
		if hasWord(word, goodWords):
			return True
	return False

	#TODO FINISH IMPLEMENTING

def getNews(str):
	#standard news outlet if none defined
	outlet = "the-wall-street-journal"

	#print(hasWord('pupy', ['ape', 'apple', 'peach']))
	findOutlet("dangus news is the best NEWS")

	requestJSON = requests.get('https://newsapi.org/v1/articles?source=the-wall-street-journal&apiKey=cbc90faf350c4cbe93ca4446bb3ff1b0')


	news = json2obj(requestJSON.text)
	articles = news.articles

	for article in articles:
		print(article.title)
	#print("I am taco %d and %s" % (taco, string))

getNews("a")