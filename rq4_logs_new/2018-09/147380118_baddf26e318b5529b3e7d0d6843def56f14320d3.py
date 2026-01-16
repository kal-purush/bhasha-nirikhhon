import requests
import json
response=requests.get('http://api.forismatic.com/api/1.0/?method=getQuote&format=json&lang=en')
data=response.json()
print(data["quoteText"])