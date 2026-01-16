#Licensed under MIT License, see root of repo
import requests, re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

url = 'https://wiki.osgeo.org/wiki/Edu_current_initiatives' #url of page
page = requests.get(url) #html
soup = BeautifulSoup(page.content, 'lxml') #parses html

tb = soup.find('table', class_ = 'wikitable sortable') #the two tables we want

#header section
headers = soup.find('tr')
new_header = []
for th in headers.find_all('th'):
    new_header.append(th.get_text().strip(',').strip())
dfheader = pd.DataFrame(new_header).transpose()

#data section
rows = soup.find_all('tr')
list_rows = []

for row in rows:
    new_row = []
    for td in row.find_all('td'):
        new_row.append(td.get_text().strip(",").strip())
    list_rows.append(new_row)
dfdata = pd.DataFrame.from_records(list_rows)

df = pd.concat([dfheader, dfdata]) #join
df.rename(columns=df.iloc[0], inplace=True) #rename headers
df.drop(df.index[0], inplace=True) #drop first row
df.drop(df.index[126], inplace=True) #drop junk row
df.drop('', axis=1, inplace=True) #remove junk column

#remove any empty rows
df['Coordinates (longitude, latitude)'].replace('', np.nan, inplace=True)
df.dropna(subset=['Coordinates (longitude, latitude)'], inplace=True) 


#regex all rows to make sure none of them are able to be split
patternDel = '.,?.'
filter = df['Coordinates (longitude, latitude)'].str.contains(patternDel)
df = df[filter]
#///

df[['long','lat']] = df['Coordinates (longitude, latitude)'].str.split(',',expand=True)#split coordinates
df.to_csv(r'E:\Documents\GitHub\gci-OSGeo-GeoForAllMap\out.csv') #out