"""This module aims to test the slicing of a string."""


import unittest


class SlicingTest(unittest.TestCase):

    text = 'Python'

    def test_create_slice(self):
        self.slice = Slicing(self.text, 1, 1, 1)

    def test_with_offset(self):
        actual_slice = Slicing(self.text, startIndex=1, endIndex=5, offset=2)
        expected_slice = 'yh'
        self.assertEqual(expected_slice, actual_slice)

    def test_without_offset(self):
        actual_slice = Slicing(self.text, startIndex=1, endIndex=4)
        expected_slice = 'yth'
        self.assertEqual(expected_slice, actual_slice)

    def test_without_end_index_and_offset(self):
        actual_slice = Slicing(self.text, startIndex=1)
        expected_slice = 'ython'
        self.assertEqual(expected_slice, actual_slice)

    def test_with_negaive_index(self):
        actual_slice = Slicing(self.text, -1)
        expected_slice = 'n'
        self.assertEqual(expected_slice, actual_slice)

    def test_with_endpoint_grater_than_lentext(self):
        actual_slice = Slicing(self.text, 0, 7, 1)
        expected_slice = 'Python'
        self.assertEqual(expected_slice, actual_slice)

    def test_with_more_argumetns(self):
        actual_slice = Slicing(self.text, 0, 3, 1, 0)
        expected_slice = 'Pyt'
        self.assertEqual(expected_slice, actual_slice)

    def test_only_with_endIndex(self):
        actual_slice = Slicing(self.text, endIndex=2)
        expected_slice = 'Python'
        self.assertEqual(expected_slice, actual_slice)

    def test_only_with_offset(self):
        actual_slice = Slicing(self.text, offset=2)
        expected_slice = 'yhn'
        self.assertEqual(expected_slice, actual_slice)

    def test_with_null_text(self):
        with self.assertRaises(IndexError):
            Slicing(None)

    def test_empty_string(self):
        with self.assertRaises(IndexError):
            Slicing("")

    def test_negative_index(self):
        with self.assertRaises(IndexError):
            Slicing(self.text, startIndex=-1)

    def test_negative_index(self):
        with self.assertRaises(IndexError):
            Slicing(self.text, startIndex=-1, endIndex=-3)

    def test_negative_index(self):
        with self.assertRaises(IndexError):
            Slicing(self.text, startIndex=len(self.text) + 1)

    def test_negative_index(self):
        with self.assertRaises(IndexError):
            Slicing(self.text, startIndex=1, endIndex=2, offset=-1)

    def test_negative_index(self):
        with self.assertRaises(IndexError):
            Slicing(self.text, startIndex=3, endIndex=1)


if __name__ == '__main__':
    unittest.main()