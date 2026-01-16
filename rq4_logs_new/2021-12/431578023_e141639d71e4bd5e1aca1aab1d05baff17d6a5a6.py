# test_unittest_calc.py
import unittest

from calc_app.calculator import Calculator


class TestCalc(unittest.TestCase):

    def test_add(self):
        self.assertEqual(Calculator.add(2, 2), 4, "Should be 4")

    def test_subtract(self):
        self.assertEqual(Calculator.subtract(10, 8), 2, "Should be 2")

    def test_multiply(self):
        self.assertEqual(Calculator.multiply(3, 5), 15, "Should be 15")

    def test_divide(self):
        self.assertEqual(Calculator.divide(10, 5), 2, "Should be 2")


if __name__ == "__main__":
    unittest.main()