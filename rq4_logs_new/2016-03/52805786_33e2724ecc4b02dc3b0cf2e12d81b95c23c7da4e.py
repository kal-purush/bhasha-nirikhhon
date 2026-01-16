import urllib2, csv 
from BeautifulSoup import BeautifulSoup

# PART 1

html = urllib2.urlopen('http://www.showmeboone.com/sheriff/jailresidents/jailresidents.asp').read()

# PART 2

soup = BeautifulSoup(html)
results_table = soup.find('table', attrs={'class': 'resultsTable'})

# PART 3

output = []

for tr in results_table.findAll('tr'):
    
    output_row = []

    for td in tr.findAll('td'):
        data = td.text.replace('&nbsp;', '')
        output_row.append(data)

    output.append(output_row)

# PART 4

with open('inmates.csv', 'w') as csvfile:
    my_writer = csv.writer(csvfile, delimiter='|')
    my_writer.writerows(output)