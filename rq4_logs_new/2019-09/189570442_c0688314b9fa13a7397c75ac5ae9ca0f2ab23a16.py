import requests
from bs4 import BeautifulSoup as BS
import csv
import codecs
import datetime


def get_html(url):
	try:
		r = requests.get(url)
	except:
		print('unable to reach URL: ' + url)
		return None
	return r.text


def get_data(html):
	if html == None:
		return None

	soup = BS(html, 'lxml')

	try:
		art = soup.find('span', {'data-qaid':'product_code'}).get_text(strip=True)
	except:
		art =''
	
	try:
		description = soup.find('span', {'data-qaid':'product_name'}).get_text(strip=True)
	except:
		description = ''
	
	try:
		price_retail = soup.find('span', {'data-qaid':'product_price'}).get_text(strip=True).replace('\xa0', '')
	except:
		price_retail = ''

	try:
		price_wholesale = soup.find('span', {'data-qaid':'wholesale_price'}).get_text(strip=True)
	except:
		price_wholesale = ''

	url = row

	date = datetime.date.today()

	print({'art':art, 'description':description, 'price_retail':price_retail, 'price_wholesale':price_wholesale, 'url':url, 'date':str(date)})
	write_csv({'art':art, 'description':description, 'price_retail':price_retail, 'price_wholesale':price_wholesale, 'url':url, 'date':str(date)})
	print('success, something was recieved')

	
def write_csv(data):
	with open('lavka_mastera_collected_data.csv', 'a', newline='', encoding='utf-16') as f:
		#newline - to avoid blank rows after each record
		#encoding utf-16 - we are in russia, thats all
		writer = csv.writer(f, delimiter=';')
		writer.writerow([data['description'], data['art'], data['price_retail'], data['price_wholesale'], data['url'], data['date']])

 
def main():
	
	with codecs.open('lavka_mastera_urls.csv', 'r', 'utf-16-le') as f:
		global row

		for row in f:
			row = row.replace('\r\n', '')
			print(row)
			try:
				#print(get_data(get_html(row[0])))
				#write_csv(get_data(get_html(row[0])))
				get_data(get_html(row))
				
			except:
				print('error, something went wrong, cant fetch info')
			print('\n')


if __name__ == '__main__':
	main()