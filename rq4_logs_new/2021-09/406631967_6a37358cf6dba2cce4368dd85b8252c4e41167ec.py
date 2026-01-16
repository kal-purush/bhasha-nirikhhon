#Gets the google page

import requests

request = requests.get("http://google.com/")

print(request)
print(request.encoding)