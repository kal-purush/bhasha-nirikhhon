import unittest
from majestoc_resa import create_app


class TestFilmService(unittest.TestCase):

    def test_something(self):
        test_app = create_app().test_client()
        res = test_app.get('/films/')
        self.assertEqual(res.status_code, 200)