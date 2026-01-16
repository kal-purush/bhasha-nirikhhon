from bs4 import BeautifulSoup
import re
soup = BeautifulSoup(open("data/rawdata/smws.html"),'html.parser')
ref_uri = re.compile("https://smwsjapan\.com/distilleries/.+")

print(soup.find_all('span',class_="product-box--title strong push-half--bottom"))

for tag in soup.find_all('a', class_="plain",href=ref_uri):
    print(tag.get('href'))


"""
<a href="https://smwsjapan.com/distilleries/isle-of-jura-distillery" class="plain">
"""