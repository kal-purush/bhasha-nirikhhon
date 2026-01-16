from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
import re
import dateutil.parser as dparser
import json
from utilities import string_clean
import os.path
from urllib.parse import urlencode
from datetime import datetime, timedelta

def fca_scraper(previous_data=None):
	chrome_options = webdriver.ChromeOptions()
	chrome_options.add_argument('--headless')

	# Build URL query
	url_endpoint = 'https://www.comcourts.gov.au/pas/public/query?'
	page_number = '1'
	query_dict = {
		'action_type': 'Intellectual_Property',
		'court': 'any',
		'file_status': 'any',
		'filed_after': (datetime.today() - timedelta(days=90)).strftime('%d/%m/%Y'),
		# Request is restricted to previous 90 days
		'filed_before': '',
		'given_name': '',
		'last_name': '',
		'page': page_number,
		'registry': 'any',
		'search_by': 'party_name'
	}
	url = url_endpoint + urlencode(query_dict)

	# Create browser driver object
	browser = webdriver.Chrome(options=chrome_options)
	browser.get(url)

	# Obtain results page count
	main = BeautifulSoup(browser.page_source, "lxml")
	results_to_loop = int(main.select('.pagination')[0].contents[-3].string)

	# Create dict to store info
	fca_filings = {"filings": []}
	# Create array to store current file_number's
	file_numbers = []

	# Loop through pages
	for i in range(results_to_loop):  # (results_to_loop):
		query_dict['page'] = str(i + 1)
		url = url_endpoint + urlencode(query_dict)

		browser.get(url)

		# Save HTML
		main = BeautifulSoup(browser.page_source, "html.parser")

		# Count links
		links_number = len(browser.find_elements_by_class_name('fileName'))

		for j in range(links_number):  # (links_number):
			sleep(0.5)
			links = browser.find_elements_by_class_name('fileName')
			links[j].click()
			print("page " + str(i + 1) + " subpage " + str(j + 1))

			subpage = BeautifulSoup(browser.page_source, "lxml")

			#
			# SCRAPING
			#

			# Filing number details
			if (subpage.find_all('th', string=re.compile("Number:")) != []):
				file_number = subpage.find_all('th', string=re.compile("Number:"))[0].next_sibling.next_sibling.text
				if (file_number == previous_data[0]):
					print("Reached last update")
					browser.quit()
					return fca_filings

				elif (file_number in previous_data or file_number in file_numbers):
					print("File already exists")
					browser.execute_script("window.history.go(-1)")
					continue

				else:
					file_numbers.append(file_number)

			else:
				print("Unable to retrieve file number details")
				file_number = ''

			# Title details
			if (subpage.find_all('th', string=re.compile("Title:"))[0].next_sibling.next_sibling != []):
				title = string_clean(subpage.find_all('th', string=re.compile("Title:"))[0].next_sibling.next_sibling.text)
				applicant = string_clean(title.split(' v ')[0])
				respondent = string_clean(title.split(' v ')[1])

				if (re.findall('ABN [0-9]+ [0-9]+ [0-9]+ [0-9]+',applicant, flags=re.I) != []):
					applicant_abn = re.findall('[0-9]+ [0-9]+ [0-9]+ [0-9]+',applicant, flags=re.I)[0]
					applicant = re.sub(' ABN [0-9]+ [0-9]+ [0-9]+ [0-9]+', '', applicant, flags=re.I)
					applicant = string_clean(applicant)
					applicant_acn = ""

				elif (re.findall('ACN [0-9]+ [0-9]+ [0-9]+', applicant, flags=re.I) != []):
					applicant_acn = re.findall('[0-9]+ [0-9]+ [0-9]+', applicant, flags=re.I)[0]
					applicant = re.sub(' ACN [0-9]+ [0-9]+ [0-9]+', '', applicant, flags=re.I)
					applicant = string_clean(applicant)
					applicant_abn = ""

				else:
					applicant_abn = ""
					applicant_acn = ""

				if (re.findall('ABN [0-9]+ [0-9]+ [0-9]+ [0-9]+',respondent, flags=re.I) != []):
					respondent_abn = re.findall('[0-9]+ [0-9]+ [0-9]+ [0-9]+',respondent, flags=re.I)[0]
					respondent = re.sub(' ABN [0-9]+ [0-9]+ [0-9]+ [0-9]+', '', respondent, flags=re.I)
					respondent = string_clean(respondent)
					respondent_acn = ""

				elif (re.findall('ACN [0-9]+ [0-9]+ [0-9]+', respondent, flags=re.I) != []):
					respondent_acn = re.findall('[0-9]+ [0-9]+ [0-9]+', respondent, flags=re.I)[0]
					respondent = re.sub(' ACN [0-9]+ [0-9]+ [0-9]+', '', respondent, flags=re.I)
					respondent = string_clean(respondent)
					respondent_abn = ""

				else:
					respondent_abn = ""
					respondent_acn = ""

			else:
				print("Unable to retrieve title details for " + str(file_number))
				title = ""
				applicant= ""
				respondent = ""

			# Filing date details
			if (subpage.find_all('th', string=re.compile("Filing Date:"))[0].next_sibling.next_sibling != []):
				filing_date_text = subpage.find_all('th', string=re.compile("Filing Date:"))[
					0].next_sibling.next_sibling.text
				filing_date_raw = dparser.parse(filing_date_text, fuzzy=True)
				filing_date = filing_date_raw.strftime('%d %B %Y')
				filing_datetime = filing_date_raw.strftime('%Y-%m-%d')
			else:
				print("Unable to retrieve date details for " + str(file_number))
				filing_date = ''
				filing_datetime = ''

			# Court details
			if (subpage.find_all('th', string=re.compile("Court:"))[0].next_sibling.next_sibling != []):
				court_text = subpage.find_all('th', string=re.compile("Court:"))[
					0].next_sibling.next_sibling.text.split(', ')
				court = string_clean(court_text[0])
				registry = string_clean(court_text[1])
			else:
				print("Unable to retrieve court details for " + str(file_number))

			if (subpage.find_all(class_="col_type apps-row")[0] != []):
				type = subpage.find_all(class_="col_type apps-row")[0].text
			else:
				print("Unable to retrieve type details for " + str(file_number))

			if (subpage.find_all(class_="col_status apps-row")[0] != []):
				status = subpage.find_all(class_="col_status apps-row")[0].text
			else:
				print("Unable to retrieve status details for " + str(file_number))

			# Click through expanders
			browser.implicitly_wait(20)
			subpage = BeautifulSoup(browser.page_source, "lxml")
			if (subpage.find(string=re.compile("Cross Claim", flags=re.I)) != []):
				cross_claim = subpage.find(string=re.compile("Cross Claim", flags=re.I))

			if (cross_claim):
				type = subpage.find_all(class_="col_type apps-row")[1].text
				browser.find_element_by_xpath('//*[@id="coa-3"]/div[1]/a').click()
				browser.implicitly_wait(20)
				browser.find_element_by_xpath('//*[@id="coa-details-3"]/div[3]/div[1]/h4/a').click()
			else:
				browser.find_element_by_xpath('//*[@id="coa-1"]/div[1]/a').click()
				browser.implicitly_wait(20)
				browser.find_element_by_xpath('/html/body/div/div[2]/div/div[3]/div[9]/div[3]/div[1]/h4/a').click()

			sleep(2)
			subpage = BeautifulSoup(browser.page_source, "lxml")

			# Applicant representative details
			if (subpage.find_all(string=re.compile('\n    \tLegal Representative Applicant')) != []):
				applicant_representative = subpage.find_all(string=re.compile('\n    \tLegal Representative Applicant'))[0].next_element.next_element.next_element.next_element.text
				applicant_representative = string_clean(applicant_representative)
			else:
				print("Unable to retreive applicant's representative details for " + str(file_number))
				applicant_representative = ''

			# Respondent representative details
			if (subpage.find_all(string=re.compile('\n    \tLegal Representative Respondent')) != []):
				respondent_representative = \
				subpage.find_all(string=re.compile('\n    \tLegal Representative Respondent'))[
					0].next_element.next_element.next_element.next_element.text
				respondent_representative = string_clean(respondent_representative)
			else:
				print("Unable to retrieve respondent's representative details for " + str(file_number))
				respondent_representative = ''

			# Construct dict object
			filing = {
				"title": title,
				"file_number": file_number,
				"filing_date": filing_date,
				"filing_datetime": filing_datetime,
				"court": court,
				"registry": registry,
				"type": type,
				"status": status,
				"applicant": applicant,
				"applicant_abn": applicant_abn,
				"applicant_acn": applicant_acn,
				"applicant_representative": applicant_representative,
				"respondent": respondent,
				"respondent_abn":respondent_abn,
				"respondent_acn":respondent_acn,
				"respondent_representative": respondent_representative
			}

			fca_filings['filings'].append(filing)

			browser.execute_script("window.history.go(-1)")

	browser.quit()
	print("FCA scrape completed")

	# Return dict object containing scraped decisions
	return fca_filings

def fca_scrape():
	# Set working directory to repo root
	os.chdir('../')

	if(os.path.isfile('Data/fca.json')):
		# Load previous scrape data
		with open('Data/fca.json', 'r') as fca_file:
			previous_data = json.load(fca_file)

		# Load previous file numbers as input data
		input_previous_data = [] # Array containing previous file numbers
		for g in range(len(previous_data['filings'])):
			input_previous_data.append(previous_data['filings'][g]['file_number'])

		# Scrape
		fca_filings = fca_scraper(input_previous_data)

		# Update file
		for i in range(len(previous_data['filings'])):
			fca_filings['filings'].append(previous_data['filings'][i])

		# Save file
		with open('Data/fca.json', 'w', encoding='utf-8') as f:
			json.dump(fca_filings, f, indent=4)

	else:
		# Scrape
		fca_filings = fca_scraper(previous_data=[0])

		# Save file
		with open('Data/fca.json', 'w', encoding='utf-8') as f:
			json.dump(fca_filings, f, indent=4)

fca_scrape()