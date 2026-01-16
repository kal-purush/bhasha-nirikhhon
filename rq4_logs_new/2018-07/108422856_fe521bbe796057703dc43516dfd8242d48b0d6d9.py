# This program deals with using requests to understand http in python.

import requests

url = "https://google.com"
reponse = requests.get(url)

printf(f"your request to {url} came back with status code {response.status_code}")

