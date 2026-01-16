from uuid import uuid4
import requests

from logger import API_URL

headers = {
  "content-type":[
    "application/json",
    "application/x-www-form-urlencoded",
  ]
}

class Logger:
  def __init__(self, token, api_url, verbose ):
    self.id = uuid4()
    self.token = token
    self.api_url = api_url | API_URL
    self.verbose = verbose | False
    self.total_logs_sent = 0

  def test_conn(self):
    headers.authorization = 'Bearer ${self.token}'
    res = requests.post(url=self.api_url+'/check', headers=headers)
    return True if res.status_code == 200 else False
  
  def log(self, type, msg):
    headers.authorization = 'Bearer ${self.token}'
    res = requests.post(url=self.api_url, headers=headers, data={type, msg})
    return True if res.status_code == 200 else False