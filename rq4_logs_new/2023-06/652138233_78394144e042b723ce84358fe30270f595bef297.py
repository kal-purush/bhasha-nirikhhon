import unittest
from solution import Solution


class TestLongestPalindrome(unittest.TestCase):
    def setUp(self):
        self.solution = Solution()

    def test_positive_case(self):
        s = "babad"
        expected_output = "bab"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_negative_case(self):
        s = "abcde"
        expected_output = "a"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_empty_string(self):
        s = ""
        expected_output = ""
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_single_char_string(self):
        s = "a"
        expected_output = "a"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_even_length_palindrome(self):
        s = "abb"
        expected_output = "bb"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_odd_length_palindrome(self):
        s = "aba"
        expected_output = "aba"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_palindrome_at_beginning(self):
        s = "abccba"
        expected_output = "abccba"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_palindrome_at_end(self):
        s = "abcdcba"
        expected_output = "abcdcba"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_long_string(self):
        s = "a" * 1000
        expected_output = "a" * 1000
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)

    def test_non_ascii_chars(self):
        s = "你好世界"
        expected_output = "你好"
        self.assertEqual(self.solution.longest_palindrome(s), expected_output)


if __name__ == '__main__':
    unittest.main()