import unittest

class TestOne(unittest.TestCase):

    def test_equals(self):
        self.assertEqual(1,1)
        self.assertEqual(2,2)

    def test_true(self):
        self.assertTrue(1)
        self.assertTrue(2==2)

    def test_contains(self):
        self.assertIn(4,[4,5,6])


if __name__ == '__main__':
    unittest.main()