import urllib, re


def main():

	start_title = raw_input('Title of Wikipedia Article (start): ')
	target_title = raw_input('Title of Wikipedia Article (target): ')

	print clickfinder(start_title, target_title)

def linkify(title):
	return 'https://en.wikipedia.org/wiki/{}'.format(title.replace(' ','_'))

def clickfinder(start_title, target_title, clicks = 1):
	''' returns the minimum number of clicks needed to get from one wikipedia
	article to another. '''


	if start_title == 'Wikipedia:Protection_policy#semi': return -1
	
	start_title, target_title = start_title.replace(' ','_'), target_title.replace(' ','_')

	start_url, target_url = [linkify(x) for x in [start_title, target_title]]

	start_file, target_file = [urllib.urlopen(url) for url in [start_url, target_url]]
	start_text, target_text = [f.read() for f in [start_file, target_file]]

	start_file.close()
	target_file.close()

	regex = '<a href="/wiki/(.+?)"'

	pattern = re.compile(regex)

	titles = re.findall(pattern, start_text)

	
	titles = filter(lambda x: (':' in x) == False, titles)
	
	#print len(titles)
	
	for title in titles:
		if title.lower().replace(' ','_') == target_title.lower().replace(' ','_'):
			return clicks

	

	for title in titles: # iterate through title links

		e = clickfinder(title, target_title)
		if e != -1:
			return e

	return -1

if __name__ == '__main__':
	main()