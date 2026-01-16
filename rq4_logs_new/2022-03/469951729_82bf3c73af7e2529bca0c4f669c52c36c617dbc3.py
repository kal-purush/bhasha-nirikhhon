import csv
import pandas as pd
import requests
import json

#Initial names of my columns
colnames = ['Date','Hour','q','i','c', 'client_name', 'client_ip', 'name','a','b','d','type','2','3']


df = pd.DataFrame(columns=['Date','Hour', 'client_name', 'client_ip', 'name','type'], index=range(17000))

#reading de log and sotre in dataframe pandas
data= pd.read_csv("queries" ,sep=" ",names=colnames)
print(data.head())

#taking the columns interested


df = pd.DataFrame(data, columns=['Date','Hour', 'client_name', 'client_ip', 'name','type'])
df["timestamp"] = df["Date"] + df["Hour"]
first_column = df.pop('timestamp')
df.insert(0, 'timestamp', first_column)
df = df.drop(["Date", "Hour"], axis=1)
print (df.head())




#print statics per client IP
ipCount=df['client_ip'].value_counts()
ipPercent=df['client_ip'].value_counts(normalize=True).mul(100).round(1).astype(str) + '%'
IP=pd.DataFrame({'counts': ipCount, 'per': ipPercent})
print(IP)

#print statics per Host
HostCount=df['name'].value_counts()
HostPercent=df['name'].value_counts(normalize=True).mul(100).round(1).astype(str) + '%'
Hosts=pd.DataFrame({'counts': HostCount, 'per': HostPercent})
print(Hosts)


#URL end point
url = 'https://api.lumu.io/collectors/5ab55d08-ae72-4017-a41c-d9d735360288/dns/queries?key=d39a0f19-7278-4a64-a255-b7646d1ace80'
myobj =   {
    "timestamp": "2021-01-06T14:37:02.228Z",
    "name": "www.example.com",
    "client_ip": "192.168.0.103",
    "client_name": "MACHINE-0987",
    "type": "A"
  } 
  

#prosesing data frame in json
js = df.to_json(orient = 'records')
parsed = json.loads(js)
for j in  range(0,10):
    print ( parsed[j])


r = requests.post(url, data =js)
print(r.status_code, r.reason)