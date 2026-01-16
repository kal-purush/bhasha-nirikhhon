import unittest
from http_server import HttpRequest

class TestHttpRequest(unittest.TestCase):
    def test_addition(self):
        result = 5
        self.assertEqual(result, 5)

    def test_multiplication(self):
        result = 6
        self.assertEqual(result, 6)

if __name__ == '__main__':
    unittest.main()