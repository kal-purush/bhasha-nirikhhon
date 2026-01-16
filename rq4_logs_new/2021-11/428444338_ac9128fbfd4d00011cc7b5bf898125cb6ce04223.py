import requests
from bs4 import BeautifulSoup
from lxml import html
from collections import Counter
from lxml import etree
import cssselect



url = "https://www.pravda.com.ua/news/2021/11/28/7315537/"
serviceNow_r = requests.get(url)
sNow_soup = BeautifulSoup(serviceNow_r.text, 'html.parser')

print(sNow_soup.find_all('href',{'class':'cta-list component'}))


for name in sNow_soup.find_all('href',{'class':'cta-list component'}):
    print(name.text)

page = requests.get(url)
tree = html.fromstring(page.content)
 
all_elms = tree.cssselect('*')
all_tags = [x.tag for x in all_elms]
 
c = Counter(all_tags)
 
# print('all:', len(all_elms), 'span:', c['span'])
 
for e in c:
    print('{}: {}'.format(e, c[e]))
 
def get_img_cnt(url):
    response = requests.get(url)
    parser = etree.HTMLParser()
    root = etree.fromstring(response.content, parser=parser)

    return int(root.xpath('count(//img)'))
print(get_img_cnt('https://www.pravda.com.ua/news/2021/11/28/7315537/'))