import unittest
from app import app
import os
import json


class UserTest(unittest.TestCase):
    def setUp(self):
        self.application = app
        self.app = self.application.test_client()
        self.user1= {
            'username': "Joseph Njuguna",
            'email':"test1@gmail.com",
            'password':'1234',
            'confirmpassword':'1234'
        }
        self.user2 ={
            'email': 'test1@gmail.com',
            'password':'1234'
        }
        self.user3= {
            'username': "Joseph Njuguna",
            'email':"",
            'password':'1234',
            'confirmpassword':'1234'
        }
        self.user4 ={
            'email': 'test1@gmail.com',
            'password':''
        }

    def test_good_auth(self):
        pass
    
    def test_good_log_in(self):
        pass
    
    def test_bad_auth(self):
        pass
    
    def test_bad_log_in(self):
        pass