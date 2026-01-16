import requests
from bs4 import BeautifulSoup
from csv import writer

categories =['']
subcategories =['']

url = "https://www.tripadvisor.com.sg/Attractions-g294265-Activities-a_allAttractions.true-Singapore.html"
r = requests.get(url)
soup = BeautifulSoup(r.content, "html.parser")


# lists = soup.find_all('article', class_ = "GTuVU XJlaI")
categories = soup.find('div', class_= "alPVI eNNhq MSadj tWKSS yzLvM")
subcategories = categories.find_all('a', class_="KoOWI")

subcategoriesLinks =[]

for subcategory in subcategories:
  subcategoriesLinks.append(subcategory['href'])
  

with open('attractions.csv', 'w', encoding = 'utf8', newline='' ) as f:
  thewriter = writer(f)
  header = ['Title', 'Attraction Type']
  thewriter.writerow(header)
  
  for link in subcategoriesLinks:
    currentlink = "https://www.tripadvisor.com.sg" + link
    print(currentlink)
    if "Attraction" not in currentlink:
      continue
    r = requests.get(currentlink)
    soup = BeautifulSoup(r.content, "html.parser")
    attractions = soup.find_all('article', class_ = "GTuVU XJlaI")
    if len(attractions) == 0:
      attractions = soup.find_all('section', class_ = "jemSU")
    category = soup.find('span', class_='kEBgK _P Cj')
    thewriter.writerow(category)
  
    for attraction in attractions:
      if attraction == None:
        continue
      
      title = attraction.find('div', class_="XfVdV o AIbhI")
      
      attraction_type = attraction.find('div', class_="biGQs _P pZUbB hmDzD")
      
      if(title != None and attraction_type != None):
        info = [title.text[title.text.find(' ') + 1:],attraction_type.text]
        
        thewriter.writerow(info)

  
  