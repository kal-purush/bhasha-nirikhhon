import requests
from bs4 import BeautifulSoup

g = requests.get("https://www.google.com")
c = g.content

scode = BeautifulSoup(c,"html.parser")
#print(scode.prettify())

#stuff = scode.find_all("div",{}) #extracts tag object, specify the name of the tag and any atrributes you want the Tag to have
#stuff = scode.find_all("a",{"class":"gb1"})
stuff = scode.find_all("a",{})
#use .find() to find the first Tag instead of returning a list of tags
#print(str(stuff[2]).split('><'))

print(stuff)#.find_all(str(id)))

#print(stuff[0].find_all("a")[0].text)