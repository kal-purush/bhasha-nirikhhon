import requests

params = {
    "token": "uclapi-bb037067b0bc025-5930d586dd754c5-97f267b79d66d0b-a3d128d4d45b62c"
}
r = requests.get("https://uclapi.com/workspaces/surveys", params=params)
print(r.json())