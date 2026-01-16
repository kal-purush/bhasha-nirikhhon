import unittest

from calculator import Calc


class TestCalc(unittest.TestCase):

    def test_1(self):
        c = Calc()
        self.assertEqual(c.display, "")

    def test_2(self):
        c = Calc()
        c.press("1")
        c.press("2")
        c.press("C")
        self.assertEqual(c.display, "")

    def test_3(self):
        c = Calc()
        c.press("1")
        self.assertEqual(c.display, "1")

    def test_4(self):
        c = Calc()
        c.press("1")
        c.press("+")
        c.press("1")
        c.press("=")
        self.assertEqual(c.display, "2")

    def test_5(self):
        c = Calc()
        c.press("1")  # 1
        c.press("+")  # 1+
        c.press("1")  # 1+1
        c.press("=")  # 2
        c.press("+")  # 2 +
        c.press("1")  # 2 + 1
        c.press("=")  # 3
        self.assertEqual(c.display, "3")

    def test_6(self):
        """1+1+1+2=5"""
        c = Calc()
        c.press("1")
        c.press("+")
        c.press("1")
        c.press("+")
        c.press("1")
        c.press("+")
        c.press("2")
        c.press("=")
        self.assertEqual(c.display, "5")

    def test_7(self):
        """1+1+1+2=5"""
        c = Calc()
        c.press("1")
        c.press("+")
        c.press("1")
        c.press("=")
        c.press("C")
        c.press("1")
        c.press("+")
        c.press("3")
        c.press("=")
        self.assertEqual(c.display, "4")


if __name__ == '__main__':
    unittest.main()