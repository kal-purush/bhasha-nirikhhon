from unittest import TestCase
import game


class TestUpperCase(TestCase):
    def test_upper_case_all_lower(self):
        name = "justin"
        actual = game.upper_case(name)
        expected = "JUSTIN"
        self.assertEqual(expected, actual)

    def test_upper_case_all_upper(self):
        name = "JUSTIN"
        actual = game.upper_case(name)
        expected = "JUSTIN"
        self.assertEqual(expected, actual)

    def test_upper_case_empty(self):
        name = ""
        actual = game.upper_case(name)
        expected = ""
        self.assertEqual(expected, actual)

    def test_upper_upper_lower(self):
        name = "jUsTiN"
        actual = game.upper_case(name)
        expected = "JUSTIN"
        self.assertEqual(expected, actual)