import requests


response = requests.get("https://reqres.in/api/users?page=2")

print(response.json())

print(response.status_code)