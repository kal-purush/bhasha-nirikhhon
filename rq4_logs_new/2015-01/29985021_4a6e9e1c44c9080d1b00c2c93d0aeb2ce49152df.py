# http://docs.python-guide.org/en/latest/scenarios/scrape/

from lxml import html
import requests

def getLinksToProfiles(url):
	# Open page
	print 'Open %s' % url
	page = requests.get(url)

	# Get content from page
	doc = html.fromstring(page.text)

	# Get the links to the profile pages
	links = doc.xpath('//div[@class="grid_6 alpha omega horz-dotted-bottom"]/div/dl/dt/a/@href') 
	print 'Found links to %s profile pages' % len(links)
	return links

def getInfoFromProfile(url):
	# Open profile page
	page = requests.get(url)

	# Get content from page
	doc = html.fromstring(page.text)

	# Get name
	name = doc.xpath('//header/div/h1/text()')[0]
	
	# Get current assignements
	assignements = doc.xpath('//article[contains(., "Aktuella uppdrag")]/div/ul/li')

	print "%s has %s assignements" % (name, len(assignements))

# Define the url to open
baseUrl = 'http://www.riksdagen.se'
url = 'http://www.riksdagen.se/sv/ledamoter-partier/Hitta-ledamot/Bokstavsordning/'

# Get links to profile pages
links = getLinksToProfiles(url)

# Open the links one by one and get data from them 
for i, link in enumerate(links):
	url = '%s%s' % (baseUrl, link)
	getInfoFromProfile(url)
	if i is 3:
		break