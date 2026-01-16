import requests
import json
from django.conf import settings

# calls in an api key that is generated from open weather platfrom
# make sure you sign up for an api key if you dont have any
open_weather_secret_key = settings.OPEN_API_KEY

# the open weather url
open_weather_url = "https://api.openweathermap.org/data/2.5/weather?"

def get_weather_date(city_name="kigali"):        
    city_url = open_weather_url + "q=" + city_name + "&appid=" + open_weather_secret_key

    print(city_url)
    # call the api url and get a respone
    response = requests.get(city_url)

    # format the data into a python json object
    res = response.json()

    # Check to see if the City exits
    if res["cod"] != "404":
        tem_kv = res['main']['temp']
        pressure = res['main']['pressure']
        humidity = res['main']['humidity']
        tem_c = int(tem_kv) - 273.15
        print(res)
    else:
        print("Sorry we cant find your city")