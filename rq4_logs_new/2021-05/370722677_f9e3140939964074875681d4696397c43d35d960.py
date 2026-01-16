import requests
from bs4 import BeautifulSoup
from notifypy import Notify
import time

notification = Notify()

def feedparse():
    URL = "https://thehackernews.com/" #enter url of the site you want to aggregate notifications from
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    result = soup.find('h2', attrs={'class':'home-title'})  #change section IDs as required
    results = result.get_text()
    notification.title = "News alert!"    
    notification.message = results  
    time.sleep(1200)
    notification.send()
    

if __name__ == '__main__':
    feedparse()






		