from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import urllib.request
import csv

#https://blog.lesjeudis.com/web-scraping-avec-python




def launchScript():

    req = Request('http://www.fasttrack.co.uk/league-tables/tech-track-100/league-table/', headers={'User-Agent': 'Mozilla/5.0'},method='GET')
    webpage = urlopen(req).read()
    soup = BeautifulSoup(webpage, 'html.parser')
    #print(soup)

    #Recherche d’éléments HTML
    # find results within table
    table = soup.find('table', attrs={'class': 'tableSorter2'})

    if table is not None:
        # Do something about x
        results = table.find_all('tr')
        print('Number of results', len(results))
        
        
        # create and write headers to a list
        rows = []
        rows.append(['Rank', 'Company Name', 'Webpage', 'Description', 'Location', 'Year end', 'Annual sales rise over 3 years', 'Sales £000s', 'Staff', 'Comments'])
        print(rows)
        
        # loop over results
        for result in results:
            # find all columns per result
            data = result.find_all('td')
            
            if len(data) !=0 :
                # write columns to variables
                rank = data[0].getText()
                company = data[1].getText()
                location = data[2].getText()
                yearend = data[3].getText()
                salesrise = data[4].getText()
                sales = data[5].getText()
                staff = data[6].getText()
                comments = data[7].getText()
                print(company)
            
            # check that columns have data
            elif len(data) == 0:
                continue
            
        
    print("End ....")