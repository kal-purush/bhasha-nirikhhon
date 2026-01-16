from bs4 import BeautifulSoup
from urlparse import urlparse, urljoin
from sys import argv

import re
import urllib2

script, filename = argv

visited = set()
# domain = ""

def get_domain_from_url(url):
  uri = urlparse(url)
  return '{scheme}://{host}'.format(scheme=uri.scheme or "http", host=uri.netloc or "localhost")

def get_full_url(url):
  return urljoin(domain, url)

# More intelligent checking to ensure no duplicate or leaked visits
def should_visit(url):
  # Domain should match
  result = get_domain_from_url(url) == domain
  
  # Should not have been visited
  result = result and not (url in visited)
  
  return result

# Deal with JS
def extract_emails_from_text(text):
  global visited
  # Step 1
  # Extract all anchor tags from text using BS4
  addresses = []
  soup = BeautifulSoup(text, "html.parser")
  anchors = soup.find_all('a')
  
  # Step 2
  # Get list of emails from text
  rendered = soup.get_text(' ')
  for word in rendered.split(' '):
    addresses += re.findall(r'[a-z]+[A-Za-z0-9._%+-]+@\S+\.com', word)
  
  # Step 3
  try:
    for a in anchors:
      url = get_full_url(a.get('href'))
      if(should_visit(url)):
        print url
        visited.add(url)
        html = urllib2.urlopen(url).read()  #the recursion starts here
        addresses += extract_emails_from_text(html) 
  except:
    pass
  
  return addresses


domain = get_domain_from_url(filename)
print domain
main = urllib2.urlopen(filename).read()
print set(extract_emails_from_text(main))