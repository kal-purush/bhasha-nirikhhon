from bs4 import BeautifulSoup
import urllib, re, zipfile, os, os.path, shutil
import zipfile

'''
Change this if the download page ever changed.
'''
download_page = 'http://download.cms.gov/nppes/'
link_page = download_page + 'NPI_Files.html'

'''
This would be prone to change if the website ever changed.
This would require modification to continue to function.
'''
zip_regex = './(NPPES_Data_Dissemination_' + '.*' + '_Weekly.zip)'

def get_download_links():
	r = urllib.urlopen(link_page).read()
	soup = BeautifulSoup(r, 'html.parser')
	links = soup.find_all('a', href=True)

	
	pattern = re.compile(zip_regex)

	weekly_links = []
	for l in links:
		path = l['href']
		m = pattern.match(path)
		if m:
			weekly_links.append(download_page + m.groups()[0])

	return weekly_links



csv_regex = 'npidata_[\\d_]*\\.csv'
def retrieve_csv_file(url):
	urllib.urlretrieve(url, 'week.zip')
	zip_ref = zipfile.ZipFile('week.zip', 'r')
	zip_ref.extractall('./week/')
	zip_ref.close()
	os.remove('week.zip')
	p = re.compile(csv_regex)
	fname = ''
	for _,_,files in os.walk('./week/'):
		for f in files:
			if p.match(f):
				os.rename('./'+f, f)
				fname = f
	shutil.rmtree('./week/')
	return fname




links = get_download_links()
retrieve_csv_file(links[0])