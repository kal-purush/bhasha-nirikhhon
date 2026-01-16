from urllib import request
from uuid import uuid4
import json
import requests

from forwarder import API_URL

# headers = {
#     "content-type": {
#         "application/json",
#         "application/x-www-form-urlencoded",
#     }
# }


class Logger:
    def __init__(self, token, api_url=API_URL, verbose=False):
        self.id = uuid4()
        self.token = token
        self.api_url = api_url
        self.verbose = verbose
        self.total_logs_sent = 0
        self.headers = ({'authorization': f'Bearer {self.token}',
                         'content-type': 'application/json'})
        print(self)

    def test_conn(self):
        res = requests.post(url=self.api_url+'/check', headers=self.headers)
        return (True, res) if res.status_code == 200 else (False, res)

    def log(self, type, msg):
        data = {"type": type, "msg": msg}
        res = requests.post(url=self.api_url, headers=self.headers, json=data)
        return (True, res) if res.status_code == 200 else (False, res)