

import pandas as pd
from sklearn import preprocessing
from collections import defaultdict
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import KMeans
from sklearn.cluster import MeanShift
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler    
from mpl_toolkits.mplot3d import Axes3D
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neural_network import MLPClassifier
import csv


arr=[]
longitude=[]
latitude=[]
coordinatesLocation=[]
t=0
q=0


#data =pd.read_csv("cities.csv", sep=',')

#print(data)


yi=defaultdict(list)
with open("statelatlong.csv", encoding="utf8") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
       #print(row[2])
       if (t>0):
           #tk=row[5].split(',')
           tk = [row[1], row[2]]
           yi[row[0]].append(tk[0])
           yi[row[0]].append(tk[-1])
       t=t+1
       

with open('../users/users_part.tsv' , encoding="utf8") as csvfile1:
    readCSV = csv.reader(csvfile1, delimiter='\t')
    for row in readCSV:
         if(q>0):
           arr.append(row[4])#(row[5]) #city column number
         q=q+1 



for i in arr:
    
     if i in yi:
        longitude.append(yi[i][0])
        latitude.append(yi[i][1])
     else:  
        longitude.append(0.0)
        latitude.append(0.0)


df=pd.DataFrame({"StateLongitude":longitude,
                "StateLatitude":latitude}) 
    
    
data =pd.read_csv('../users/users_part.tsv' , sep='\t')

rt = data.join(df)


rt.to_csv('../users/users_part.tsv', sep='\t', index = False)    
    
'''from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="myAPP")

for i in arr:
    location = geolocator.geocode(i)
    #print((location.latitude, location.longitude))
    longitude.append(location.longitude)
    latitude.append(location.latitude)
    #coordinatesLocation.append((location.latitude, location.longitude))

df=pd.DataFrame({"longitude":longitude,
                 "latitude":latitude})    
print(df)'''    