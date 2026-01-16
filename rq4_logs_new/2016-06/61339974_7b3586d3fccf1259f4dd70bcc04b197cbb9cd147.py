import urllib
import urllib.request

data = {}
data[''] = 'BT'

url_values = urllib.parse.urlencode(data)
url = "http://www.hacg.li/wp/?s"
full_url = url + url_values

#data = urllib.request.urlopen(full_url).read()
#data = data.decode('UTF-8')
print(url_values)
print(full_url)
print(data)