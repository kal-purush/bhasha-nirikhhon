import requests
import re
from bs4 import BeautifulSoup


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


itemKeyword = input("Type item to find in stock: ")
itemToFindURL = itemKeyword.replace(' ', '+')
itemToFind = itemKeyword

URL = 'https://www.amazon.com.mx/s?k=' + itemToFindURL + '&__mk_es_MX=ÅMÅŽÕÑ&ref=nb_sb_noss_2'

print(URL)

headers = {
    "User-Agent": 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
}

print("Looking for: " + itemToFind + "\n-----------------------------------")

page = requests.get(URL, headers=headers)

if page.status_code == 200:
    print("status code: ", bcolors.OKBLUE + str(page.status_code) + bcolors.ENDC)
else:
    print("status code: ", bcolors.FAIL + str(page.status_code) + bcolors.ENDC)

soup = BeautifulSoup(page.text, 'lxml')

foundItem = 0

print("Item to find: ", itemToFind)

for div in soup.find_all('div',
                         class_='sg-col-4-of-24 sg-col-4-of-12 sg-col-4-of-36 s-result-item s-asin sg-col-4-of-28 '
                                'sg-col-4-of-16 sg-col sg-col-4-of-20 sg-col-4-of-32'):
    foundItemThis = 0
    for img in div.find_all('img',
                            alt=True):
        if re.search(r'.*' + itemToFind + '.*', img['alt'], re.IGNORECASE):
            print(bcolors.OKBLUE + "FOUND!" + bcolors.ENDC)
            print(bcolors.OKGREEN + img['alt'] + bcolors.ENDC)
            foundItem = 1
            foundItemThis = 1
    if foundItemThis == 1:
        spanPrice = div.find('span', class_='a-price')
        if spanPrice:
            price = spanPrice.find('span', class_='a-offscreen')
            print(bcolors.BOLD + price.text + bcolors.ENDC)
        for span in div.find_all('span', class_='aok-inline-block s-image-logo-view'):
            print(bcolors.UNDERLINE + "Available with prime!" + bcolors.ENDC)

if foundItem == 0:
    print(bcolors.WARNING + "No items found!" + bcolors.ENDC)

print(bcolors.BOLD + "finished..." + bcolors.ENDC)
input(bcolors.HEADER + "Press Enter to continue..." + bcolors.ENDC)