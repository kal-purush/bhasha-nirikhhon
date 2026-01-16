import os

import geocoder
import requests

# Using OpenWeather API to handle weather forecast processing
API_KEY = os.environ.get("API_KEY")

def get_weather_data(city, country="", unit="imperial"):
    if country == "":
        r = requests.get(f"https://api.openweathermap.org/data/2.5/find?q={city}&appid={API_KEY}&units={unit}")
    else:
        r = requests.get(f"https://api.openweathermap.org/data/2.5/find?q={city},{country}&appid={API_KEY}&units={unit}")

    try:
        for data in r.json()["list"]:
            name = data["name"]
            temperature = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            country = data["sys"]["country"]
            rain = data["rain"]
            snow = data["snow"]
            for info in data["weather"]:
                description = info["description"]

            return name, country, temperature, humidity, rain, snow, description
    except:
        return None

def auto_detect_loc():
    g = geocoder.ip("me")
    return get_weather_data(g.city, g.country)