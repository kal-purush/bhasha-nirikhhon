import json
import urllib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def qc_wind(wind_data):
    num_rec = len(wind_data)
    index = wind_data.index.values
    gust = list(wind_data['wind_gust_set_1'])
    spd = list(wind_data['wind_speed_set_1'])
    plt.figure()
    plt.plot(gust)
    print(sum(np.isnan(gust)))
    qc_dump = []
    qc_bool=0
    for i in range(num_rec-1):
        #check if wind gust is 3x greater than wind speed
        if gust[i]>=10.0*spd[i]:
            gust[i]=np.nan
            spd[i]=np.nan
            qc_dump.append(index[i])
            qc_bool=1
        if (gust[i]>=(40+spd[i])):
            gust[i]=np.nan
            spd[i]=np.nan
            qc_dump.append(index[i])
            qc_bool=1
        if np.isnan(spd[i]) and gust[i]>60:
            gust[i]=np.nan
            spd[i]=np.nan
            qc_dump.append(index[i])
            qc_bool=1

        if i!=0:
            if gust[i-1]!=0.0  and gust[i+1]!=0.0:
                if gust[i]/gust[i-1]>=5.0 or gust[i]/gust[i+1]>=5.0:
                    gust[i]=np.nan
                    qc_dump.append(index[i])
                    qc_bool=1
           
    plt.figure()             
    plt.plot(gust)
    plt.ylim([0,50])
    plt.figure()
    plt.plot(spd)
    plt.ylim([0,50])
   
    return gust,spd 
    
    
TOKEN = 'b1c91e501782441d97ac056e2501b5b0'

args = {'sensorvars':'1',
            'stids': 'C1',
            'token': TOKEN
            }

#use the url library to retrieve the data
apiString = urllib.parse.urlencode(args)
web_service_url = 'http://api.mesowest.net/v2/stations/metadata'
full_url = web_service_url + '?' + apiString
print(full_url)

# Open the URL and convert the returned JSON into a dictionary
web = urllib.request.urlopen(full_url)
response = json.loads(web.read())
start_date = response['STATION'][0]['PERIOD_OF_RECORD']['start']
end_date = response['STATION'][0]['PERIOD_OF_RECORD']['end']

args = {'start': 199912020000,
        'end': 201807101300,
        'obtimezone': 'UTC',
        'qc': 'on',
        'stids': 'BBEC1',
        'token': TOKEN
        }

apiString = urllib.parse.urlencode(args)
web_service_url = "http://api.mesowest.net/v2/stations/timeseries"
full_url = web_service_url + '?' + apiString

# Open the URL and convert the returned JSON into a dictionary
web = urllib.request.urlopen(full_url)
response = json.loads(web.read()) 


date_time = response['STATION'][0]['OBSERVATIONS']['date_time']
varlist = list(response['STATION'][0]['OBSERVATIONS'].keys())
site_data = {v: response['STATION'][0]['OBSERVATIONS'][v] for v in varlist}
df = pd.DataFrame(index=date_time,data=site_data)
df.index = pd.to_datetime(df.index)

wind_data = df[['wind_gust_set_1','wind_speed_set_1']]
gust, spd = qc_wind(wind_data)