import requests
from bs4 import BeautifulSoup
import random
# columbus salami: 073007107034

def lookup(barcode):

    # list of user agents to not raise alert
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)',
        'Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'
    ]

    # pick a random user agent
    user_agent = random.choice(user_agent_list)

    # initialize header with user agent
    headers = {
        "User-Agent": user_agent
    }

    # create URL with entered barcode
    URL = "https://us.openfoodfacts.org/product/"+str(barcode)

    # grab html content and apply beautifulsoup
    page = requests.get(URL, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")

    # search for name of product
    raw_name = soup.find('title')
    name = raw_name.text.strip()
    print(f"Product: {name}")

    # search for fat contents
    result = soup.find(id='nutriment_fat_tr')

    # search for values within fat contents
    inner_result = result.find_all(class_="nutriment_value")

    # iterate over all values and insert into list initalized with type in 0 place.
    # format [type, 100g value, serving value, extra info...]
    nutrition_list = ['fat']
    for nutriment_value in inner_result:
        nutrition_list.append(nutriment_value.text.strip())

    # grab necessary values
    nutri_type = nutrition_list[0]
    nutri_100g = nutrition_list[1]
    nutri_ss = nutrition_list[2]

    print(f"Type: {nutri_type}")
    print(f"Serving Size: {nutri_ss}")
    print(f"100g: {nutri_100g}")
