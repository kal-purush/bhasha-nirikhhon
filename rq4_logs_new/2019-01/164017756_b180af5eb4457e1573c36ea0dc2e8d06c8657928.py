import unittest
import os
import json
from app import app

class MeetupTest(unittest.TestCase):
    def setUp(self):
        self.appliaction= app
        self.app = self.appliaction.test_client()
        self.record1={
            'topic' : "AI",
            'location' : "iHub Upperhill,Nairobi Kenya " ,
            'happeningOn' : "Sat 14th Jan 2019",
        }
    def test_get_record(self):
        pass
    
    def test_user_authorized(self):
        pass

    def test_not_found(self):
        pass