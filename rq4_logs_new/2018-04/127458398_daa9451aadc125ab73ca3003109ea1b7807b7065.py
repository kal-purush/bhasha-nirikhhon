import unittest

from slackertracker.app import create_app, db
from slackertracker.tests.suite import test_config

class TestHello(unittest.TestCase):
    def setUp(self):
        app = create_app(test_config)
        app.testing = True
        self.app = app.test_client()
        with app.app_context():
            db.create_all()


    def test_hello(self):
        response = self.app.get('/')
        assert b'Hello!' in response.data

    def tearDown(self):
        db.session.remove()
        db.drop_all()