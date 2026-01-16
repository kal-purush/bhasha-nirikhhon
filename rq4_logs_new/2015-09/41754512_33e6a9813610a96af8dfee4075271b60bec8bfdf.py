from bs4 import BeautifulSoup
import requests
import sys
import re

def crawler(page,city,fob):
	try:
		soup1 = BeautifulSoup(s.get(page).text)
	except requests.exceptions.RequestException as e:
		print e
		sys.exit(1)	
	finalPageList = [span.find("a")['href'] for span in soup1.find_all("span",{"class":"jcn"})]
	for link in finalPageList:
		try:
			soup = BeautifulSoup(s.get(link).text)
		except requests.exceptions.RequestException as e:
			print e
			sys.exit(1)	
		name = soup.find("span",{"class":"fn"})
		if name:
			name = name.text.strip()
		else:
			name = "n/a"
		phone = soup.find("a",{"class":"tel"})
		if phone:
			phone = phone.text.strip()
		else: 
			phone = "n/a"
		address = soup.find("span",{"class":"jaddt"})
		if address:
			address = re.sub('\s+',' ', address.text)
		else:
			address = "n/a"
		estd = soup.find("span",{"class":"rtngna"})
		if estd:
			estd = estd.text.strip()
		else:
			estd = "n/a"
		hrs_of_work1 = soup.find("div",{"class":"hrsop"})
		if hrs_of_work1:
			hrs_of_work = re.sub('\s+',' ', hrs_of_work1.text)
		else:
			hrs_of_work = 'n/a'
		website1 = soup.find("section",{"class":"moreinfo"})
		website2 = website1.find("p",{"class":"wsurl"})
		if website2:
			website = website2.find("a")['href']
		else:
			website = "n/a"
		modeOfPayment = "n/a"
		for header in website1.find_all("section",{"class":"fcont"}):
			if header.find(text = "Modes of Payment"):
				modeOfPayment = re.sub('\s+',' ',header.text)
				modeOfPayment = re.sub('Modes of Payment','',modeOfPayment)

		try:
			fob.write('''"'''+name.encode('utf-8').strip()+'''","'''+\
				phone.encode('utf-8').strip()+'''","'''+address.encode('utf-8').strip()+\
				'''","'''+city+'''","'''+estd.encode('utf-8').strip()+'''","'''+\
				hrs_of_work.encode('utf-8').strip()+'''","'''+website.encode('utf-8').strip()+\
				'''","'''+modeOfPayment.encode('utf-8').strip()+'''",\n''')
		except:
			pass

	nextLink = soup1.find("a",{"rel":"next"})
	if nextLink:
		if nextLink.has_attr('href'):
			crawler(nextLink['href'],city,fob)
		else:
			return 0
	else:
		return 0

profession = '57/Doctors_b2c'
with open('./cities.txt','r') as cities:
    cityList = [city.strip() for city in cities]
page = []
fob = open("./out.csv",'wb')
fob.write('''"Name","phone_number","Adress","City","Estd","Opening Hrs.","Web-site","Payment-Mode",\n''')

for city in cityList:
	url1 = "http://www.justdial.com/"+city+'/'+profession+'/page-1'
	url2 = "http://www.justdial.com/"+city+'/'+profession+'/page-2'
	try:
		soup1 = BeautifulSoup(requests.get(url1).text)
		soup2 = BeautifulSoup(requests.get(url2).text)
	except requests.exceptions.RequestException as e:
		print e
		sys.exit(1)
	page = page + [a['href'] for a in soup1.find_all("a", {"class": "jd_rating_f_o"}) if a.has_attr('href')]
	page = page + [a['href'] for a in soup2.find_all("a", {"class": "jd_rating_f_o"}) if a.has_attr('href')]
	for landingPages in page:
		s = requests.Session()
		crawler(landingPages,city,fob)

fob.close()