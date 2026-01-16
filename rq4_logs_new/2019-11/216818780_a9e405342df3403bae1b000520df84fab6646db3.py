# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 17:45:45 2019

@author: luchi
"""

from bs4 import BeautifulSoup
import urllib.request

url = urllib.request.urlopen("https://www.emag.ro/dezvoltare-personala/c?ref=grid")
page = url.read()
soup = BeautifulSoup(page, 'html.parser') 
print(soup)

name = soup.find('a', attrs = {'class':'product-title js-product-url'})['title']
print(name)

book_container = soup.find_all('div', class_ = 'card-section-wrapper js-section-wrapper')
print(type(book_container))
print(len(book_container))


#Redeclaring the lists to store data in
book_names = []
new_price = []
reviews = []
    
# Parse the content of the request with BeautifulSoup
soup = BeautifulSoup(page, 'html.parser') 
# Select all the 100 movie containers from a single page
book_container = soup.find_all('div', class_ = 'card-section-wrapper js-section-wrapper')

print(book_container)
        # For every movie of these 100
for container in book_container:
        name = container.h2.a.text
        book_names.append(name)
         #if container.find('p', class_ = 'product-new-price') is not None:  
        #NEW_PRICE
        price = container.find('p', class_ = 'product-new-price').get_text(strip=True)
        a = price.replace('Lei', '')
        a = float(a)
        new_pr = a/100
        new_price.append(new_pr) 
        #REVIEWS
        review = container.find('span', class_ = 'hidden-xs')
        reviews.append(review)
 
import pandas as pd       
books = pd.DataFrame({'BookName': book_names,
                        'New price' : new_price,
                        'Reviews' : reviews
                        })
print(books.info())
books.head(10)


        
        
        
        
        
        
        
        
        