import requests
from datetime import datetime

USERNAME = ""
TOKEN = ""

pixela_endpoint = "https://pixe.la/v1/users"
user_params = {
    "token": TOKEN,
    "username": USERNAME,
    "agreeTermsOfService": "yes",
    "notMinor": "yes",
}

# 2. Create your user account:
# response = requests.post(url=pixela_endpoint, json=user_params)
# print(response.text)


# 3. Create a graph definition:
graph_endpoint = f"{pixela_endpoint}/{USERNAME}/graphs"

graph_params = {
    "id": "graph1",
    "name": "Running Graph",
    "unit": "Km",
    "type": "float",
    "color": "sora",
}

header = {
    "X-USER-TOKEN": TOKEN
}

response = requests.post(url=graph_endpoint, json=graph_params, headers=header)


# 4. Post value to the graph
post_endpoint = f"{pixela_endpoint}/{USERNAME}/graphs/{graph_params["id"]}"

today = datetime.now()
post_params = {
    "date": today.strftime("%Y%m%d"),
    "quantity": "11.45",
}

# post_response = requests.post(url=post_endpoint, json=post_params, headers=header)
# print(post_response.text)

update_endpoint = f"{pixela_endpoint}/{USERNAME}/graphs/{graph_params["id"]}/{today.strftime('%Y%m%d')}"
new_post_params = {
    "quantity": "9.45",
}

post_response = requests.put(url=update_endpoint, json=new_post_params, headers=header)
# print(post_response.text)