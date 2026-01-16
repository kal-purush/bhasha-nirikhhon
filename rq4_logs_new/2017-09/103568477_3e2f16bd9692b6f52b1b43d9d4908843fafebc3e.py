import urllib.request, json
import datetime

def TakeWeather():
    with urllib.request.urlopen("http://api.openweathermap.org/data/2.5/weather?q=Ulyanovsk,ru&appid=4ccf6f0e364ccbaf2562da6856712f2b") as url:
        data = json.loads(url.read().decode())
        weather_status = data['weather'][0]['main']
        weather_description = data['weather'][0]['description']
        wind_speed = data['wind']['speed']
        sunrise = data['sys']['sunrise']
        sunrise = datetime.datetime.fromtimestamp(sunrise).strftime('%Y-%m-%d %H:%M:%S')
        sunset = data['sys']['sunset']
        sunset = datetime.datetime.fromtimestamp(sunset).strftime('%Y-%m-%d %H:%M:%S')
        result = {
            'status':weather_status,
            'description':weather_description,
            'windspeed':wind_speed,
            'sunrise':sunrise,
            'sunset':sunset
        }
        return result

print(TakeWeather())