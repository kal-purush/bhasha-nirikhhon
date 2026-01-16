'''NCSC Public Report Submission'''
import datetime
import json
import shortuuid
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# Define the required scopes
scopes = [
  "https://www.googleapis.com/auth/firebase.database"
]

# Authenticate a credential with the service account
credentials = service_account.Credentials.from_service_account_file(
    "serviceAccountKey.json", scopes=scopes)


# Create access token
request = Request()
credentials.refresh(request)
access_token = credentials.token
print("Access token",access_token)


# Create a random UUID
uidd = shortuuid.ShortUUID().random(length=11)

# Data to be submitted
data = { uidd: {

    "title":"API public report of today",
    "fullName":"",
    "email":"email@gmail.com",
    "phoneNumber":"",
    "description":"This was added using python application through the realtime DB API",
    "uidd":uidd,
    "date":str(datetime.datetime.today())}
}

# Convert data to json
data_json = json.dumps(data)

# Patch request to add a public report to reports
patch = requests.patch("https://ncsc-2622e-default-rtdb.europe-west1.firebasedatabase.app/reports.json",
data=data_json, headers={'Authorization':access_token})

print(patch)