import requests
from requests import exceptions
from pprint import pprint
from Constants import *
import cPickle
import sys


class Object(object):
    def __init__(self, **response):
        for k, v in response.iteritems():
            if isinstance(v, dict):
                self.__dict__[k] = Object(**v)
            else:
                self.__dict__[k] = v


class Credentials(object):
    def __init__(self, request_url, hmh_api_key, client_id, grant_type, username, password, refresh_token=None):
        assert isinstance(request_url, (str, unicode)), request_url
        assert isinstance(hmh_api_key, (str, unicode)), hmh_api_key
        assert isinstance(client_id, (str, unicode)), client_id
        assert isinstance(grant_type, (str, unicode)), grant_type
        assert isinstance(username, (str, unicode)), username
        assert isinstance(password, (str, unicode)), password
        self.request_url = request_url
        self.hmh_api_key = hmh_api_key
        self.client_id = client_id
        self.grant_type = grant_type
        self.username = username
        self.password = password
        if refresh_token:
            assert isinstance(refresh_token, (str, unicode)), refresh_token
        self.refresh_token = refresh_token

    def get_response(self):
        values = {
            "client_id": self.client_id,
            "grant_type": self.grant_type,
            "username": self.username,
            "password": self.password
        }
        if self.grant_type == "refresh_token" and self.refresh_token:
            values["refresh_token"] = self.refresh_token
        try:
            r = requests.post(self.request_url, values)
            r.raise_for_status()
            return r.json()
        except exceptions.RequestException as e:
            print e
            sys.exit(-1)

    @staticmethod
    def to_object(response):
        return Object(**response)

    @staticmethod
    def save_as(tokens, filename):
        return cPickle.dump(tokens, open(filename, "wb"))


if __name__ == "__main__":
    # client = Credentials(request_url=BASE_URL + ACCESS_TOKEN_URL,
    #                      hmh_api_key=HMH_CLIENT_API_KEY,
    #                      client_id=CLIENT_ID,
    #                      grant_type="password",
    #                      username="sauron",
    #                      password="password")
    # resp = client.get_response()
    # client.save_as(resp, "sauron_credentials.pkl")
    credentials = cPickle.load(open("sauron_credentials.pkl", "rb"))
    url = BASE_URL + V1_STUDENT_INFO
    headers = {
        "Vnd-HMH-Api-Key": HMH_CLIENT_API_KEY,
        "Authorization": credentials["access_token"],
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    r = requests.get(url=url, headers=headers)
    pprint(r.json())