from urllib.request import Request, urlopen

import requests
from bs4 import BeautifulSoup

user=input("Enter the username: ")
quote_page='https://www.codechef.com/users/%s'%(user)
#print(quote_page)


req = requests.get(quote_page, headers={'User-Agent': 'Mozilla/5.0'})
if(req.status_code==200):
    webpage = req.text
#print(webpage)

    soup=BeautifulSoup(webpage,'html.parser')
    rating=soup.find('div',{'class':'rating-number'})
    rating=rating.text.strip()
    div=soup.find('div',{'class':'rating-ranks'})
    ranking=div.find_all('a')
    
   
    side_nav=soup.find('ul',{'class':'side-nav'})
    span=side_nav.find_all('span')
    label=side_nav.find_all('label')
    name=span[len(label)+2]
    name=name.text.strip()
    name=name[17:len(name)]

   
    print("Name of the user is %s"%(name))
    print("The codechef rating of the user is %s"%(rating))
    print("Global ranking of user is %s"%(ranking[0].text.strip()))
    print("Country ranking of user is %s"%(ranking[1].text.strip()))
    for i in range(2,len(label)-1):
        cl=label[i].text.strip()
        cs=span[i+3].text.strip()
        print("%s %s"%(cl,cs))

   
  

    
else:print("The user does not exist.Enter a valid username")
