import requests
from datetime import datetime

location= input("Enter the city name:")
apikey="3b1796cb3ea88c8eb01ddf6ad8925328"

complete_api_link="https://api.openweathermap.org/data/2.5/weather?q="+location+"&appid="+apikey
api_link=requests.get(complete_api_link)
api_data= api_link.json()

if api_data['cod']=='404':
    print("Invalid city:{}, please check your city name".format(location))
else:
    
    temp_city = ((api_data['main']['temp'])- 273.15)
    weather_desc=api_data['weather'][0]['description']
    hmdt=api_data['main']['humidity']
    wind_spd= api_data['wind']['speed']
    date_time= datetime.now().strftime("%d %b %Y | %I:%M:%S %p")
    
    print("------------------------------------------------------------")
    print("weather stats for -{} ||".format(location.upper(),date_time))
    print("------------------------------------------------------------")
    
    print("current temperature is: {:.2f} deg C".format(temp_city))
    print("current weather desc  :",weather_desc)
    print("Current Humidity      :",hmdt,'%')
    print("Current wind speed    :",wind_spd,'kmph')
    
