import requests
from datetime import datetime

HOST = 'http://75.38.110.170:5000/'
ROUTE = '/devicedata/all/0/'

date = str(datetime.today().date())
date = date.replace('-','_')

#Replace variables after each : to your variables. Keep deviceid to 0
try:
    req = requests.post(HOST+ROUTE+date,json={"timestamp": current_timestamp,
                                              "date": d,
                                              "deviceid": 0,
                                              "temperature": temp,
                                              "windspeed": random.randint(0,100),
                                              "winddirection": current_wind,
                                              "humidity": round(random.uniform(0.45,0.55),2)*100,
                                              "pressure": random.randint(0,100),
                                              "aqi": aqi
                                              })
    print(req.content)
except:
    print(f"Error Posting {current_timestamp}") #replace with your timestamp variable