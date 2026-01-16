
from flask import Flask, request
from flask_cors import CORS
import requests, json
from semantic_search import Search
app = Flask(__name__)
CORS(app)

@app.route('/search/<string:language>/<string:search_query>')
def doSearch(language,search_query):
	t=Search()
	#print(t.search(language,search_query))
	#print(search_object.search("c++", "print hello world"))
	languageName=language
	searchThis=search_query.replace("_"," ")
	#return search_query
	print(language)
	print(search_query)
#	print (t.search(language,searchThis))
	return t.search(language,searchThis)
#	return t.search(language,search_query)

#@app.route('/search/<language>/<search_query>')
#def search(language, search_query):
 #       language_code = get_language_codes(language)
  #      url = f"https://searchcode.com/api/codesearch_I/?q={search_query}&p=1&per_page1&lan={language_code}"
   #     headers = {'Accept': 'application/json'}
#
 #       result = requests.get(url, headers = headers).json()
  #      
   #     return result['results'][0]['lines'] # Returns code of first search result