import unittest
import json
from app import app
import os

class QuestionsTest(unittest.TestCase):
    def setUp(self):
        self.application = app
        self.app = self.application.test_client()

        self.question1={
            'user':1,
            'meetup' :1,
            'title' :"Entrance Ticket Price",
            'body' : "How much will be paid by each person for entrance"
        }
        self.question2={
            'user':1,
            'meetup' :1,
            'title' :"",
            'body' : "How much will be paid by each person for entrance"
        }
        self.question3={
            'user':1,
            'meetup' :1,
            'title' :"Entrance Ticket Price",
            'body' : ""
        }
        self.question4={
            'user':1,
            'meetup' :None,
            'title' :"Entrance Ticket Price",
            'body' : "How much will be paid by each person for entrance"
        }
        self.question4={
            'user':"",
            'meetup' :2,
            'title' :"Entrance Ticket Price",
            'body' : "How much will be paid by each person for entrance"
        }
    def test_qood_question_post(self):
        response = self.app.post('/api/v1/questions', data= json.dumps(self.question1), content_type='application/json')
        self.assertEqual(response.status_code, 201)

    def test_no_tittle_question_post(self):
        pass

    def test_no_body_question_post(self):
        pass

    def test_no_meetup_id(self):
        pass

    def test_invalid_user_id(self):
        pass
            