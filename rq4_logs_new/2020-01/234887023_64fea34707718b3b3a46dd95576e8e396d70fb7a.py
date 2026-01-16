import requests
import json

url = "http://transport.opendata.ch/v1/stationboard?station=Malters&limit=1"
response = requests.get(url)
data = response.text

parsed = json.loads(data)
station = parsed["station"]


print(station)
print(data)