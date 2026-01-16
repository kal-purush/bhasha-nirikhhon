"""This module aims to test Concatenate() method.
It is just the first version, I did not include yet the any exceptions."""

import unittest

class ConcatenationTests(unittest.TestCase):

    def test_concatenate_existence(self):
        self.Concatenate = Concatenate("aaaa")

    def test_concatenate_two_lowercase_letters(self):
        first_value = "a"
        second_value = "b"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'ab'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_two_uppercase_letters(self):
        first_value = "A"
        second_value = "B"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'AB'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_lowercase_letter_and_uppercase_letter(self):
        first_value = "a"
        second_value = "B"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'aB'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_uppercase_letter_and_lowercase_letter(self):
        first_value = "A"
        second_value = "b"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'Ab'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_letter_and_string(self):
        first_value = "a"
        second_value = "1"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'a1'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_string_and_letter(self):
        first_value = "1"
        second_value = "A"
        actual_value = Concatenate(first_value, second_value)
        expected_value = '1A'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_string_and_special_character(self):
        first_value = "1"
        second_value = "!"
        actual_value = Concatenate(first_value, second_value)
        expected_value = '1!'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_letter_and_special_character(self):
        first_value = "A"
        second_value = "!"
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'A!'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_letter_and_integer(self):
        first_value = "A"
        second_value = 2
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'A2'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_special_character_and_integer(self):
        first_value = "!"
        second_value = 2
        actual_value = Concatenate(first_value, second_value)
        expected_value = '!2'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_letter_and_float(self):
        first_value = "A"
        second_value = 2.1
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'A2.1'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_special_character_and_float(self):
        first_value = "!"
        second_value = 2.1
        actual_value = Concatenate(first_value, second_value)
        expected_value = '!2.1'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_special_character_and_empty_value(self):
        first_value = "!"
        second_value = ""
        actual_value = Concatenate(first_value, second_value)
        expected_value = '!'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_letter_and_empty_value(self):
        first_value = "a"
        second_value = ""
        actual_value = Concatenate(first_value, second_value)
        expected_value = 'a'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_empty_value_and_integer(self):
        first_value = ""
        second_value = 2
        actual_value = Concatenate(first_value, second_value)
        expected_value = '2'
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_string_and_null_atr(self):
        first_value = None
        second_value = "a"
        actual_value = Concatenate(first_value, second_value)
        expected_value = "a"
        self.assertEqual(actual_value, expected_value)

    def test_concatenate_three_strings(self):
        first_value = "1"
        second_value = "A"
        third_value = "!"
        actual_value = Concatenate(first_value, second_value, third_value)
        expected_value = "1A!"
        self.assertEqual(actual_value, expected_value)

if __name__ == '__main__':
    unittest.main()