import unittest

from flask import url_for

from todoism import create_app, db
from todoism.models import User, Item


class APITestCase(unittest.TestCase):

    def setUp(self):
        app = create_app('testing')
        self.app_context = app.test_request_context()
        self.app_context.push()
        db.create_all()
        self.client = app.test_client()

        user = User(username='yang')
        user.set_password('123')
        item = Item(body='Test Item', author=user)

        user2 = User(username='tao')
        user2.set_password('123')
        item2 = Item(body='Test Item 2', author=user2)

        db.session.add_all([user, item, user2, item2])
        db.session.commit()

    def tearDown(self):
        db.drop_all()
        self.app_context.pop()

    def get_oauth_token(self):
        response = self.client.post(url_for())