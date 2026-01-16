import unittest
from step2_eval import *


class TestStep2(unittest.TestCase):
    def test_EVAL(self):
        self.assertEqual(EVAL(1), 1)
        self.assertEqual(EVAL(["+", 1, 1]), 2)
        self.assertEqual(EVAL(["+", 11, 11]), 22)
        self.assertEqual(EVAL(["+", 1, ["+", 0, 0]]), 1)
        self.assertEqual(EVAL(["+", 1, ["+", 0, ["+", 0, 0]]]), 1)

        self.assertEqual(EVAL(READ("(+ 1 1)")), 2)
        self.assertEqual(EVAL(["+", -1, 1]), 0)
        self.assertEqual(EVAL(READ("(+ -1 1)")), 0)
        self.assertEqual(EVAL(READ("(* -3 6)")), -18)

    def test_eval_ast(self):
        pass


if __name__ == "__main__":
    unittest.main()