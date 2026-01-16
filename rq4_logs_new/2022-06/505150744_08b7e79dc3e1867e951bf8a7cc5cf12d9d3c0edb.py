import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
import requests
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.utils import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


url = 'https://github.com/huseyn-guliyev/cron_bloomberg_scraper/tree/main/data'
response = requests.get(url)
w_soup =  BeautifulSoup(response.text, 'html.parser')
txt = w_soup.find_all('a', attrs={'class':'js-navigation-open Link--primary'})
txt = list(map(lambda x: x.get('title'), txt))
if len(txt) == 1:
    x = 0
else:
    txt.remove('00.txt')
    # x = int(txt[1].get('title').split('_')[-1][:-7])
    x = max(list(map(lambda x: int(x.split('_')[-1][:-4]), txt)))


chrome_service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())

chrome_options = Options()
options = [
    "--headless",
    "--disable-gpu",
    "--window-size=1920,1200",
    "--ignore-certificate-errors",
    "--disable-extensions",
    "--no-sandbox",
    "--disable-dev-shm-usage"
]
for option in options:
    chrome_options.add_argument(option)

driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

urls = pd.read_csv('bloomberg_news_2021_july_2022_june.csv')['0']
# urls = df['0'].iloc[x:x+150]

for x in range(x, x+150):
    try:
        driver.get(urls[x])
        html=driver.page_source
        soup =  BeautifulSoup(html, 'html.parser')
        headlines = []
        summaries = []
        categories = []
        pillars = []
        authors = []
        dates = []

        try:
            headlines.append(soup.find_all('h1', attrs = {"class":'headline__699ae8fb'})[0].text)  ## headline
        except:
            headlines.append('__error__')

        try:
            summary = ''
            for f in soup.find_all('div', attrs = {"class":'abstract-item-text__d2d4dde8'}):
                summary += f.text  ## summaries
                summary += ' ____next____ '

            summaries.append(summary)

        except:
            summaries.append('__error__')

        try:
            categories.append(soup.find_all('span', attrs = {"class":'brand__3ac459ef'})[0].text)  ## category
        except:
            categories.append('__error__')

        try:
            pillars.append(soup.find_all('div', attrs = {"class":'pillar__a08f2d74'})[0].text) ## pillar
        except:
            pillars.append('__error__')

        try:
            author = ''
            for a in soup.find_all('p', attrs = {"class":'author__619cf27c'}):
                author += a.text  ## authors
                author += ' ____next____ '
            authors.append(author)
        except:
            authors.append('__error__')
            
        try:
            dates.append(soup.find_all('div', attrs = {"class":'lede-times__03902805'})[0].text)  ## date and time
        except:
            dates.append('__error__')
    except:
        pass

result = pd.DataFrame({'headlines': headlines,
             'summaries': summaries,
             'categories': categories,
             'pillars': pillars,
             'authors': authors,
             'dates':dates})

result.to_csv('./data/till_{}.csv'.format(x+150))