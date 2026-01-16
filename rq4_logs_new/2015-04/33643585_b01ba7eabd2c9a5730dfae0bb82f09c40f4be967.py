from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import re
import csv
import time

with open('DalYelp.csv','w', newline='') as fp:
    a = csv.writer(fp, delimiter=',')
    data = [['Name','Rating','Reviews','Dollars','Neighborhood','Category']]
    a.writerows(data)
    
    j=900
    for i in range(0,10):
        driver = webdriver.Firefox()
        driver.get('http://www.yelp.com/search?find_desc=restaurants&find_loc=Dallas%2C+TX&ns=1#start='+str(j))
        time.sleep(5)
        html = driver.page_source
        soup = BeautifulSoup(html)

        data = soup.find_all("div",{"class":"search-result natural-search-result"})

        str1 = []

        for item in data:
            str1.append(item.contents)
        
        namestr = []
        ratstr = []
        revstr = []
        dolstr = []
        neighstr = []
        catstr = []

        for i in range(0,len(str1)):
            namestr.append(re.search(r'img alt=".+?"',str(str1[i])).group())
            ratstr.append(re.search(r'title=".+?"',str(str1[i])).group())
            revstr.append(re.search(r'\d+ reviews',str(str1[i])).group())
            try:
                dolstr.append(re.search(r'>.+?</span>',str(str1[i])).group())
            except:
                dolstr.append('N/A')
            try:
                neighstr.append(re.search(r'<span class="neighborhood-str-list">\s.+?</span>',str(str1[i])).group())
            except:
                neighstr.append('N/A')
            try:
                catstr.append(re.search(r'<span class="category-str-list">\n.*>.*</a>.*',str(str1[i])).group())
            except:
                catstr.append('N/A')

        for i in range(0,len(namestr)):
            data2 = [[namestr[i],ratstr[i],revstr[i],dolstr[i],neighstr[i],catstr[i]]]
            a.writerows(data2)
            i+=1
        j+=10
        driver.quit()