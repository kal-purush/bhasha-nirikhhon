import unittest


class Solution:
    """
    This solution uses dynamic programming to compute the nth Fibonacci
    number faster than a standard recursive approach would. Since the
    nth Fibonacci number is the sum of the two previous numbers by
    definition, it's possible to just maintain the two previous numbers
    in variables and keep summing them together until you reach the nth
    number.
    """
    def fib(self, N: int) -> int:

        first = 0
        second = 1
        for _ in range(N):
            temp = first + second
            first = second
            second = temp
        return first


class TestSolution(unittest.TestCase):

    def test_first_example(self):

        self.assertEqual(Solution().fib(4), 3)

    def test_second_example(self):

        self.assertEqual(Solution().fib(8), 21)