import requests, sys, urllib2
from bs4 import BeautifulSoup as BS

def chunk_report(bytes_so_far, chunk_size, total_size):
	percent = float(bytes_so_far) / total_size
	percent = round(percent*100, 2)
	sys.stdout.write("Downloaded %d of %d bytes (%0.2f%%)\r" % (bytes_so_far, total_size, percent))
	if bytes_so_far >= total_size:
		sys.stdout.write('\n')

def chunk_read(response, chunk_size=8192, report_hook=None):
	total_size = response.info().getheader('Content-Length').strip()
	total_size = int(total_size)
	bytes_so_far = 0
	while 1:
		chunk = response.read(chunk_size)
		bytes_so_far += len(chunk)
		if not chunk:
			break
		if report_hook:
			report_hook(bytes_so_far, chunk_size, total_size)
	return bytes_so_far

def episode_download(url):
	file_name = url.split('/')[-1]
	req = urllib2.Request(url)
	req.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36')
	req.add_header('Cookie','video=true;_ga=GA1.2.339709105.1424606219;_ororo_session=RlVKU2JUYWkyUzIvQUJGMTMxZ0JKcExzM2pxSWdzcEdCWEw3RlNDbmh5UkpaeDFqeC9jR1Y1WGNJSHgzazVhLzJFaGNLOG4wR0hsdlBHcGtQZjBhTHY3ZHB1WVdralB5UjdzNFdJQVlmdU5lcG4xN2dLT3c2WTNvRFpNNWtaMGRsV2V2enFXT0hqZTBoNzJPV3JiaGNMWHFOcFUzVmV6VkthQTlGaFEzYlFLeDg3bG8wZWxydTF4eHBnY1Z5VmF1a1VsNVJsQU9ySlhMYTNXR1VGNzREZmV6aURUMzBPeXRTWENBM2psM0dzcz0tLXc0VjlBZjUrV3hlSTUxSTRDTEk0RHc9PQ%3D%3D--c030986d158d66528261ff4297006f14e1fc0d87')
	#headers={'User-Agent':user_agent}
	#opener.addheaders.append(headers)
	try:
		u = urllib2.urlopen(req)
		chunk_read(u, report_hook=chunk_report)

	except urllib2.HTTPError, e:
		print e.fp.read()


def main():

	if len(sys.argv)<3:
		print "Run like python ororo.py <series-name> <season>\nExample:\tpython ororo.py arrow 1"
		exit()
	_series=sys.argv[1]
	_season=sys.argv[2]
	print _series+" "+_season
	html_content=urllib2.urlopen('http://ororo.tv/en/shows/'+_series+'#'+_season).read()

	soup=BS(html_content)
	links=soup.find_all("a",attrs={"class":"episode"})
	print "List of Episodes\n"
	for episode in links:
		epi=(episode.get('href'))
		season=epi.split("-")[0][1:]
		if season==_season:
			print episode.text
	for episode in links:
		epi=(episode.get('href'))
		season=epi.split("-")[0][1:]
		if season==_season:
			link=(episode.get('data-href'))
			link='http://ororo.tv'+link
			html_content=urllib2.urlopen(link).read()
			soup=BS(html_content)
			dl_link=soup.find('source')
			dl_link=dl_link.get('src')
			print dl_link
			episode_download(dl_link)
			
if __name__=="__main__":
	main()			