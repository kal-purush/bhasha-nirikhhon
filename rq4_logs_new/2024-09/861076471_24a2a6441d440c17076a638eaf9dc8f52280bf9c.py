import requests
import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenWeatherMap API configuration
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITY = "Kano,NG"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def kelvin_to_celsius(kelvin):
    return kelvin - 273.15

def get_weather_data():
    params = {
        "q": CITY,
        "appid": API_KEY,
    }
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data: {response.status_code}")
        return None

def display_weather_report(weather_data):
    if weather_data:
        main = weather_data['main']
        weather = weather_data['weather'][0]
        wind = weather_data['wind']
        
        print("\n=== Weather Report for Kano, Nigeria ===")
        print(f"Date and Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Weather: {weather['main']} ({weather['description']})")
        print(f"Temperature: {kelvin_to_celsius(main['temp']):.1f}°C")
        print(f"Feels Like: {kelvin_to_celsius(main['feels_like']):.1f}°C")
        print(f"Min Temperature: {kelvin_to_celsius(main['temp_min']):.1f}°C")
        print(f"Max Temperature: {kelvin_to_celsius(main['temp_max']):.1f}°C")
        print(f"Humidity: {main['humidity']}%")
        print(f"Pressure: {main['pressure']} hPa")
        print(f"Wind Speed: {wind['speed']} m/s")
        print(f"Wind Direction: {wind['deg']}°")
        if 'rain' in weather_data:
            print(f"Rainfall (last 1h): {weather_data['rain'].get('1h', 0)} mm")
        print("=====================================")
    else:
        print("Unable to fetch weather data.")

def main():
    if not API_KEY:
        print("Error: OpenWeatherMap API key not found. Please set it in the .env file.")
        return
    
    weather_data = get_weather_data()
    display_weather_report(weather_data)

if __name__ == "__main__":
    main()