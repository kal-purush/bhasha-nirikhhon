import scrappy
from mongoengine import *



class WebResource(Document):
	homepageDescription = StringField()
	pageTitle = StringField()
	link = StringField(required=True, unique=True)
	linkName = StringField()
	sourceType = StringField(default="page")
	sourceDescription = StringField(default="not available")



class demoCrawler(scrappy.Spider):
	name = "demo_crawler"
	start_urls = ["https://epiinfovhf.codeplex.com/documentation"]
	allowed_domains = ["https://epiinfovhf.codeplex.com/documentation"]
	def parse(self, response):
		pageTitle = parseTitle(response)
		pageDescription = parseDescription(response)
		videoes = parseVideoLinks(response)
		books = parseBooks(response)
		for video in videoes:
			newResource = WebResource(homepageDescription=PageDescription,
									pageTitle=pageTitle,
									link = video[0],
									linkName=video[1],
									sourceType="video")
			newResource.save()
		
		for book in books:
			newResource = WebResource(homepageDescription=pageDescription,
									pageTitle=pageTitle,
									link = book[0],
									linkName=book[1],
									sourceType="book")
			newResource.save()

		



def parseTitle(response):
	title = response.xpath("//h1//text()").extract_first(default="not available")
	return title

def parseDescription(response):
	texts = response.xpath("//html//p//text()").extract()
	return " ".join(texts)

def parseVideoLinks(response):
	toReturn = []
	videoSelectors = response.xpath("//a[contains(text(), 'Video')]|//a[contains(text(), 'video')]")
	for selector in videoSelectors:
		toReturn.append((selector.xpath("@href").extract_first(), selector.xpath("text()").extract_first(default="videolink")))
	return toReturn

def parseBooks(response):
	toReturn = []
	booksSelectors = response.xpath("//a[contains(text(), 'book')]|//a[contains(text(), 'Book')]")
	for selector in booksSelectors:
		toReturn.append((selector.xpath("@href").extract_first(), selector.xpath("text()").extract_first(default="booklink")))
	return toReturn



