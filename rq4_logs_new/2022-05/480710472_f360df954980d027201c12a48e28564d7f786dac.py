# Author: Tianchen Wang
# Get the details of restaurant.
# SQLite & Python 3.7

import json
import requests

class getRestaurantDetail:

    miseName = ["Chinese"]
    API_KEY = "AIzaSyCh7xnktNn-nWh_KlNh_VP8rg8DITmqL9Q"

    f = open(f"/users/xiaogeamadeus/mypy/Practice_of_Information_System/groulette_backend/restaurantData/kafugenDetail.txt",
             mode='a+')
    payload = {}
    headers = {}

    url = "https://maps.googleapis.com/maps/api/place/details/json?" \
              "place_id=ChIJ-TIGS_EJAWARlNerteIGYsc&language=ja&" \
              f"key={API_KEY}"

    response = requests.request("GET", url, headers=headers, data=payload)
    num = f.write(response.text)
    f.close()