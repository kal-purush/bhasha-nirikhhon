# Import dependencies

import pandas as pd
from bs4 import BeautifulSoup
import requests
from splinter import Browser
import time

# Initialize browser

def init_browser():
    executable_path = {'executable_path': 'chromedriver.exe'}

    # For Mac Users:
    # executable_path = {"executable_path": "/usr/local/bin/chromedriver"}

    return Browser('chrome', **executable_path, headless=False)

# The following function will scrape various dog related websites for data and return a Python dictionary of the data collected

def scrape():

    # Initialize browser

    browser = init_browser()

    # Create an empty dicitonary to store scraped dog restaurant data

    dog_scrapped_data = []

    ############################

    # Bring Fido dog friendly restaurants (https://www.bringfido.com/restaurant/city/san_francisco_ca_us/)

    dog_url = "https://www.bringfido.com/restaurant/city/san_francisco_ca_us/"
    browser.visit(dog_url)
    time.sleep(3)
    dog_html = browser.html
    dog_soup = BeautifulSoup(dog_html, 'html.parser')
    restaurant_names = dog_soup.findall("div", class_="info").find("span").text

    for restaurant in restaurant_names:
        dog_scrapped_data.append(restaurant)
    
    browser.quit()

    return dog_scrapped_data


