import requests
import webbrowser
from bs4 import BeautifulSoup

URL = 'https://curiouspumpkincom.wordpress.com/blog/'
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')
print(soup)

results = soup.find_all('h2',class_='entry-title')
print(results)

title_url={}
num = 0
for i in results:
    num += 1
    title = i.find('a').text
    title_url[title] = i.find('a')['href']
    print(num, ":", title)

Blog_post=input("Enter the title of your page: ")

if Blog_post in title_url:
    webbrowser.open(title_url[Blog_post])
else:
    print("Invalid title. Please try again.")