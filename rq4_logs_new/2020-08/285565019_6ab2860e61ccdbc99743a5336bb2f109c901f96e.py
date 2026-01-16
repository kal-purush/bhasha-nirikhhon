# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 13:33:51 2019

@author: LENOVO
"""

# startup funding case study question1 
'''Your Friend has developed the Product and he wants to establish the product startup
 and he is searching for a perfect location where getting the investment has a high chance.
 But due to its financial restriction, he can choose only between three 
 locations -  Bangalore, Mumbai, and NCR. As a friend, you want to help your friend 
 deciding the location. NCR include Gurgaon, Noida and New Delhi. Find the location 
 where the most number of funding is done. That means, find the location where
 startups has received funding maximum number of times. Plot the bar graph between
 location and number of funding. Take city name "Delhi" as "New Delhi". 
 Check the case-sensitiveness of cities also. That means, at some place instead of 
 "Bangalore", "bangalore" is given. Take city name as "Bangalore". For few startups
 multiple locations are given, 
one Indian and one Foreign. Consider the startup if any one of the city lies in given locations.'''

import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt

df=pd.read_csv("startup_funding.csv")
df['Date'].replace('12/05.2015','12/05/2015',inplace = True)
df['Date'].replace('13/04.2015','13/04/2015',inplace = True)
df['Date'].replace('15/01.2015','15/01/2015',inplace = True)
df['Date'].replace('22/01//2015','22/01/2015',inplace = True)
def convert_date(date):
    return date.split('/')[-1]

df['Year']=df['Date'].apply(convert_date)
year_count=df['Year'].value_counts()
year_fund=list(zip(year_count.index,year_count.values))
year_fund=np.array(year_fund,dtype=int)
year_fund=year_fund[year_fund[:,0].argsort()]
year=year_fund[:,0]
funding_round=year_fund[:,1]

####  
plt.plot(year,funding_round,marker='o')
plt.xticks(year)
plt.title('Year vs No. of Funding_round')
plt.xlabel('Year')
plt.ylabel('Funding round')
plt.show()
for i in range(len(year)):
    print(year[i],funding_round[i])
    
#####
# question specific


df['CityLocation'].dropna(inplace=True)
def separateCity(city):
    return city.split('/')[0].strip()
df['CityLocation']=df['CityLocation'].apply(separateCity)
df['CityLocation'].replace("Delhi","New Delhi",inplace=True)
df['CityLocation'].replace("bangalore","Bangalore",inplace=True)
city_number=df['CityLocation'].value_counts()[0:10]

city=[]
no_fundings=[]
for i in city_number.index:
    if i =="Bangalore":
        city.append(i)
        no_fundings.append(city_number[i])
    if i =="Mumbai":
        city.append(i)
        no_fundings.append(city_number[i])
    if i =="New Delhi":
        city.append(i)
        no_fundings.append(city_number[i])
    if i =="Noida":
        city.append(i)
        no_fundings.append(city_number[i])
    if i =="Gurgaon":
        city.append(i)
        no_fundings.append(city_number[i])
        

## plotting
plt.bar(city,no_fundings)
plt.xticks(city)
plt.title('City vs no of Fundings')
plt.xlabel('City------>')
plt.ylabel('Funding round ')
plt.show()


## Answer to question 1:
# from the bar graph we can see that , out of the preferred locations (Bangalore, Mumbai and NCR) 
# Bangalore has the most number of fundings. 

# but if we consider New Delhi, Noida  and Gurgaon as one location , then the case is different

city.append("NCR")
no_fundings.append(city_number["New Delhi"]+city_number["Noida"]+city_number["Gurgaon"])
        



    



