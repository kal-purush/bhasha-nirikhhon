#!/usr/bin/env python

#Instantiate scraper class
wapo = WaPoScraper()

# Base url to crawl from
base_url = 'http://www.washingtonpost.com'

# Get urls by crawling
urls = wapo.get_urls(10)

# Get article headlines and tags
articles, labels = wapo.get_labels()

for i in range(len(articles)):
    print(headline[i])
    print(labels[i])