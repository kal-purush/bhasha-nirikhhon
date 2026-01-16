#Code to print_all_links. 

def get_next_target(page):
	start_link = page.find('<a href=')
	if start_link == -1: # there's no a href links present in the webpage
		return None, 0
	start_quote = page.find('"', start_link) # returns a number that indicates the positioning of
	'"' after the positioning of start_link
	#refer to string = '"foo"This is a string to test link <a href="www.google.com"> so that it"bar"'
	#>>> string.find('"', 4, 10)
	#4
	#>>> string.find('"', 0, 10)
	#0
	#>>> string.find('"', 5, 10)
	#-1
	#>>> string.find('"', 5)
	#43
	#>>> string.find('"', 43)
	#43
	#>>> string.find('"', 44)
	#58
	#>>> string.find('"', 59)
	#71
	end_quote = page.find('"', start_quote + 1) 
	url = page[start_quote + 1:end_quote]
	return url, end_quote

def print_all_links(page):
	while True:
		url, endpos = get_next_target(page) #multiple assignment: 
		if url: 
			print url
			page = page[endpos:] #update the page to keep the loop going
		else:
			break