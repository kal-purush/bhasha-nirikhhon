from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import json
import datetime
import random
import re

random.seed(datetime.datetime.now())

def getLinks(articleUrl):
    html = urlopen("http://en.wikipedia.org"+articleUrl)
    bsObj = BeautifulSoup(html)
    return bsObj.find("div", {"id":"bodyContent"}).findAll("a", href=re.compile("^(/wiki/)((?!:).)*$"))

def getHistoryIPs(pageUrl):
    #Format of history pages is: http://en.wikipedia.org/w/index.php?title=Title_in_URL&action=history
    pageUrl = pageUrl.replace("/wiki/", "")
    historyUrl = "http://en.wikipedia.org/w/index.php?title="+pageUrl+"&action=history"
    print("history url is: "+historyUrl)
    html = urlopen(historyUrl)
    bsObj = BeautifulSoup(html)
    #finds only the links with class "mw-anonuserlink" which has IP addresses instead of usernames
    ipAddresses = bsObj.findAll("a", {"class":"mw-anonuserlink"})
    addressList = set()
    for ipAddress in ipAddresses:
        addressList.add(ipAddress.get_text())
    return addressList

def getCountry(ipAddress):
    try:
        response = urlopen("http://freegeoip.net/json/"+ipAddress).read().decode('utf-8')
    except HTTPError:
        return None
    responseJson = json.loads(response)
    return responseJson.get("country_name")

links = getLinks("/wiki/Python_(programming_language)")

while(len(links) > 0):
    for link in links:
        print("-------------------") 
        historyIPs = getHistoryIPs(link.attrs["href"])
        for historyIP in historyIPs:
            country = getCountry(historyIP)
            if country is not None:
                print(historyIP+" is from "+country)

    newLink = links[random.randint(0, len(links)-1)].attrs["href"]
    links = getLinks(newLink)
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import datetime
import random

pages=set()
random.seed(datetime.datetime.now())

#内链
def getInternalLinks(bsObj,includeUrl):
    internalLinks=[]
    for link in bsObj.findAll('a',href=re.compile("^(/|.*'+includeUrl+')")):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in internalLinks:
                internalLinks.append(link.attrs['href']);
    return internalLinks

#外链
def getExternalLinks(bsObj,excludeUrl):
    excludeLinks=[]
    for link in bsObj.findAll('a',href=re.compile("^(http|www)(?!'+excludeUrl+'.)*$")):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in excludeLinks:
                excludeLinks.append(link.attrs['href'])
    return excludeLinks

def splitAddress(address):
    addresspart=address.replace("http://","").split('/')
    return addresspart

def getRandomExternalLink(startingPage):
    html=urlopen(startingPage)
    bsObj=BeautifulSoup(html)
    externalLinks=getExternalLinks(bsObj,splitAddress(startingPage)[0])
    if len(externalLinks)==0:
        internalLinks=getInternalLinks(bsObj,splitAddress(startingPage)[0])
        return internalLinks[random.randint(0,len(internalLinks)-1)]
    else:
        return externalLinks[random.randint(0,len(externalLinks)-1)]

def followExternalOnly(startingSite):
    externalLink=getRandomExternalLink("http://oreilly.com")
    print("随机外链是："+externalLink)
    followExternalOnly(externalLink)

followExternalOnly("http://oreilly.com")

    
                
        
            