import requests
import json
import json
import boto3

S3_PATH = "dataminded-academy-capstone-resources"
url = "https://api.openaq.org/v2/latest?limit=100&page=1&offset=0&sort=desc&radius=1000&order_by=lastUpdated&dumpRaw=false"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

json_response = json.loads(response.text)
with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(json_response, f, ensure_ascii=False, indent=4)

s3 = boto3.resource('s3')
s3object = s3.Object(S3_PATH, 'Johan@pxl/ingest/airquality_latest.json')

s3object.put(
    Body=(bytes(json.dumps(json_response).encode('UTF-8')))
)