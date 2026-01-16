import requests

# OpenWeatherMap API Key
API_KEY = 'f18acc43e2da39df93a8293f004bea83'  # Replace with your actual API key

# Location (Bhubaneswar)
location = "Bhubaneswar"

# Fetch live weather data for Bhubaneswar
weather_url = f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={API_KEY}&units=metric'
response = requests.get(weather_url)
data = response.json()

# Check if the response is successful (status code 200)
if data['cod'] == 200:
    # Extract relevant weather data
    temperature = data['main']['temp']        # Temperature in °C
    windspeed = data['wind']['speed']         # Wind speed in m/s
    humidity = data['main']['humidity']       # Humidity percentage
    rainfall = data.get('rain', {}).get('1h', 0)  # Rainfall in mm (last hour)

    print(f"Weather data for Bhubaneswar:")
    print(f"Temperature: {temperature}°C")
    print(f"Wind Speed: {windspeed} m/s")
    print(f"Humidity: {humidity}%")
    print(f"Rainfall (last hour): {rainfall} mm")
else:
    print(f"Error: {data.get('message', 'Could not fetch data')}")