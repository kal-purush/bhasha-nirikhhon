from bs4 import BeautifulSoup
import requests
import re

vedant_products = {}

def vedant_result(name):
    vedant_item = name.replace(" ","%20")
    vedant_url = "https://www.vedantcomputers.com/index.php?route=product/search&search=" + vedant_item
    vedant_link = requests.get(vedant_url)
    soup = BeautifulSoup(vedant_link.content,"lxml")
    html_titles = soup.find_all(class_='name')
    html_prices = soup.find_all(class_='price')
    vedant_prices = []
    for price in html_prices:
        vedant_prices.append(price.text)
    vedant_names = []
    for name in html_titles:
        vedant_names.append(name.text)
    print("Results from Vedant : ")
    if html_titles == [] or html_prices == []:
        print("No products matched your search: " + name )
        return
    for name,price in zip(vedant_names,vedant_prices):
        vedant_products[name] = price
        print(name + " : " + price)
    
    min_price = min(vedant_prices)
    print_min(min_price)

def get_name(val,products):
    for key,value in products.items():
        if val==value:
            return key

def print_min(min_price):
    print("\nThe minimum priced product is : " + get_name(min_price,vedant_products) + " : "+ min_price)


if __name__ == "__main__":
    name = input("Enter the item to compare prices for : ")
    vedant_result(name)
    print()