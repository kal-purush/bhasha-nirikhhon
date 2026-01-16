import json
from os import environ

from locust import HttpLocust, TaskSet, task

class UserBahaviour(TaskSet):
    token = None
    base_url = '/api/v1'
    admin_email = environ.get('EMAIL_HOST_USER')
    admin_password = environ.get('EMAIL_HOST_PASSWORD')
    flight = None

    def on_start(self):
        self.signin()
    
    def auth_header(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
            'content-type': 'application/json'
        }
    
    def post_request(self, data, url):
        response = self.client.post(f"{self.base_url}/{url}", data)
        return response
    
    def get_request(self, url):
        response = self.client.get(f"{self.base_url}/{url}", headers=self.auth_header())
        return response
    
    def patch_request(self, data, url, kwargs={}):
        response = self.client.patch(f"{self.base_url}/{url}", data, **kwargs)
        return response

    def signin(self):
        data = {
            "email": self.admin_email,
            "password": self.admin_password
        }
        response = self.post_request(data, 'users/signin/')
        self.token = json.loads(response.content)['data']['token']
    

    @task(1)
    def entry_poinnt(self):
        self.get_flights()
        self.purchase_ticket()
        self.get_tickets()
        self.get_user()

    def get_flights(self):
        response = json.loads(self.get_request('flights').content)
        self.flight = response['data'][0]
        return self.flight

    def purchase_ticket(self):
        
        passengers = [
            {
                'name': 'Passenger 1',
                'email': 'passenger@test.com',
                'nationality': 'Nigeria',
                'issuarance_country': 'Nigeria',
                'passport_no': 'AWERTY6UIJ',
                'expiration_date': '2030-02-02',
                'telephone': '1234567890'}
            ]

        ticket_data = {
            'passengers': passengers,
            'form_of_payment': 'Cash',
            'flight_id': self.flight['id']
        }

        response = self.client.post('/api/v1/tickets/', json.dumps(ticket_data), headers=self.auth_header())
        return response

    def get_tickets(self):
        response = self.get_request('tickets')
        return response

    def get_user(self):
        response = self.get_request('users/profile')
        return response


class FlightyAPILocust(HttpLocust):
    task_set = UserBahaviour
    min_wait = 2000
    max_wait = 5000