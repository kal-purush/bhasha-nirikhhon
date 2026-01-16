from bs4 import BeautifulSoup
import requests
from csv import writer

url = "https://www.careerguide.com/career-options"

page = requests.get(url)

soup = BeautifulSoup(page.content, 'html.parser')

values = soup.find_all('div', class_='c-content-panel')
header = soup.find_all('h2', attrs={'class': 'c-font-bold'})
list_values = soup.find_all('ul', attrs={'class': 'c-content-list-1 c-theme c-separator-dot'})
sub_catagory = soup.find_all('ul', attrs={'class': 'c-content-list-1 c-theme c-separator-dot'})

with open('dataset_carrierguide.csv', 'w', encoding='utf-8', newline='') as f:
    the_writer = writer(f)
    for value1 in header:
        the_writer.writerow(value1)
        for value2 in list_values:
            the_writer.writerows(value2)



# I will furthur try to clean this data in colab 