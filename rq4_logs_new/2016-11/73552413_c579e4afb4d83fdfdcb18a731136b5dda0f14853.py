"""This module aims to test the lowercase method."""


import unittest


class Lower(unittest.TestCase):

    def test_lower_existence(self):
        self.Lower = Lower("aaaa")

    def test_lowercase_null(self):
        with self.assertRaises(IndexError):
            Lower(None)

    def test_lowercase_null(self):
        with self.assertRaises(IndexError):
            Lower("")

    def test_lowercase_null(self):
        with self.assertRaises(TypeError):
            Lower("!23~")

    def test_lowercase_lower_value(self):
        text = "aaaa"
        actual = Lower(text)
        expected = "aaaa"
        self.assertEqual(actual, expected)

    def test_lowercase_lower_value(self):
        text = "AAAA"
        actual = Lower(text)
        expected = "aaaa"
        self.assertEqual(actual, expected)

    def test_lowercase_lower_value(self):
        text = "1anDv"
        actual = Lower(text)
        expected = "1andv"
        self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()