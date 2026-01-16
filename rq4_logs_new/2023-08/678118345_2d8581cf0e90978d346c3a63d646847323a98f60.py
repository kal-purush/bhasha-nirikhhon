import requests 
import json

response=requests.get('https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&site=stackoverflow')

# Printing response 200
print(response)

# Getting covered data (from response)
print(response.json())

# Indexed for items
print(response.json()['items'])

# Getting questions with links 
for question in response.json()['items']:
    print(question['title'])
    print(question['link'])
    print()

