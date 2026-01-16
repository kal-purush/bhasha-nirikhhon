#!/usr/bin/python3

### Load Webpages into python through REQUESTS

import requests as req
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/Blockchain"
r = req.get(URL)
print(r.content[:200])

#print the source code in proper format
soup = BeautifulSoup(r.content)
print(soup.prettify())


#### Access elements and attributes inside HTML pages (example: h1, img, h2, ul, div, li, table)
print(soup.h1)
print(soup.img)

tables = soup.find_all("table")         # find all tables on webpage
print(len(tables))
print(tables[1])
tables[1]["style"]                      # print the css contents of second table

lists = soup.find_all("li")
print(len(lists))
childs = list(lists[3].children)
print(len(childs))
print(childs)


### Search for elements with given class and attributes
links = soup.find_all("a")              # get all links from the webpage
print(len(links))
links[0:5])
#Now if we wanna filter out links of a particular class, apart from using Brute force (for loop)
attr_filter = {'class': "<name>"}
soup.find_all{"a", attr_filter}
#type 2
attr_filter = {'class': "mw-jump-link",
               'href' : "#mw-head"}
soup.find_all{"a", attr_filter}
#type 3 (not meant for printing)
attr_filter = {'class': "noprint"}
soup.find_all{None, attr_filter}        # to find all elements
#type 4
attr_filter = {'class': "fn"}           # for css
soup.find_all{None, attr_filter}
#type 5
attr_filter = {'id': "firstHeading"}         
soup.find_all{None, attr_filter}


### Using browser dev tools as a toolkit
## Using CSS Selectors for searching complex patterns

#Download specific image source code using CSS Selector.... Go to webpage > Image > Inspect Element > Right-Click > Copy > Selector
selector = "#mw-content-text > div.mw-parser-output > div:nth-child(49) > div > a > img"
soup.select(selector)
#Thumbnail images on the webpage
selector = ".thumbimage"
soup.select(selector, limit=3)


### Send POST, PUT and PATCH data with modified headers
import json

api_link = "https://jsonplaceholder.typicoder.com/posts"

#GET request
r = req.get(api_link)
data = json.loads(r.content)
print(data[0:2])
#POST request
r = req.post(api_link)
data = json.loads(r.content)
print(data)
#POST 2
input_data = {"title": "test title",
              "user_id": 5}
r = req.post(api_link, input_data)
data = json.loads(r.content)
print(data)
#PUT request    ----- BLANK OUTPUT {}
input_data = {"title": "test title",
              "user_id": 5}
r = req.put(api_link, input_data)
data = json.loads(r.content)
print(data)

#PATCH request  ----- BLANK OUTPUT {}
input_data = {"title": "test title",
              "user_id": 5}
r = req.patch(api_link, input_data)
data = json.loads(r.content)
print(data)

#Changing Headers in a POST request for JSON
input_data = json_dumps({"title": "test title",
              "user_id": 5})
headers = {"Content-Type": "application/json"}
r = req.post(api_link, input_data, headers=headers)
data = json.loads(r.content)
print(data)


### Authenticate and maintain connection state through sessions and cookies
link = "http://testing-ground.scraping.pro/login"

LOGGED_SELECTOR = '#cases_login > h3'

def is_logged(html_source):
        soup = BeautifulSoup(html_source)
        elements = soup.select(LOGGED_SELECTOR)
        if len(elements) == 1: 
                elements = elements[0]
                if 'success' in element.get('class', []):
                        return True
                else:
                        return False
        elif len(elements) > 1:
                raise Exception('Something is wrong with the source')
        else:
                return False
                
input_data = {"usr": "admin",
              "pwd": "12345"}
r_post = req.post(link + "?mode=login", input_data}
is_logged(r_post.content)        # TRUE, till here only as it is logged in but the state is not saved

#SAVE Session Cookie
s = req.Session()
r_post = s.post(link + "?mode=login", input_data}
r_get = s.get(link + "?mode=welcome"}   #This would've resulted in a FALSE if not used Session()
is_logged(r_post.content)       # TRUE from now on