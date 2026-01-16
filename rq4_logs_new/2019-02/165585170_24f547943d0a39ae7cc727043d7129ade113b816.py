import json
from os import environ

from locust import Locust, TaskSet, task

class UserBahaviour(TaskSet):
    token = None
    base_url = '/api/v1/'
    admin_email = environ.get('EMAIL_HOST_USER')
    admin_password = environ.get('EMAIL_HOST_PASSWORD')

    def on_start(self):
        self.signup()
    
    def auth_header(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token)
        }
    
    def post_request(self, data, url):
        response = self.client.post(f"{self.base_url}/{url}", data)
        return response
    
    def get_request(self, url):
        response = self.client.post(f"{self.base_url}/{url}", data, headers=self.auth_header())
        return response
    
    def patch_request(self, data):
        pass

    def signup(self):
        data = {
            "email": self.admin_email,
            "password": self.admin_password
        }
        response = self.post_request(data. 'users/signin/')
        self.token = json.loads(response._content).get('token')
    
    @task(2)
    def get_flights(self):
        response = self.get_request('flights')



class FlightyAPILocust(Locust):
    task_set = UserBahaviour
    min_wait = 5000
    max_wait = 15000