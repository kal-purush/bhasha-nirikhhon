import unittest
import math
from Classes.Function import Function

class FunctionTests(unittest.TestCase):

    function = Function()

    def setUp(self):
        function = Function()

    def test_sin(self):
        self.assertEqual(self.function.sin(0), math.sin(0))

    def test_performOp(self):
        x = 1
        y = 2
        self.assertEquals(self.function.performOp('+', x, y), x + y)
        self.assertEquals(self.function.performOp('-', x, y), x - y)
        self.assertEquals(self.function.performOp('*', x, y), x * y)
        self.assertEquals(self.function.performOp('/', x, y), x / y)

    def test_evalOperand(self):

        equation  = ['1', '+', '1', '*', '2']
        self.assertEquals(self.function.evalOperand(equation.index('*'), equation), 1 * 2 )
        self.assertNotEquals(self.function.evalOperand(equation.index('*'), equation), 1 + 1 * 2 )

        self.assertEquals(equation, [1 * 2])
