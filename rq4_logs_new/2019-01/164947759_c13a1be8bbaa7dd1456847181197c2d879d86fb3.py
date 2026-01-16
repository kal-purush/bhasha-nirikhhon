'''
Provides calculator functionality (+, -, *, /) DKA
'''

from .exceptions import InsufficientOperands


class Calculator(object):
    '''Provides calculator functionality (+, -, *, /) DKA'''

    def __init__(self, adder, subtracter, multiplier, divider):
        '''init calculator functions and stack DKA'''

        self.adder = adder
        self.subtracter = subtracter
        self.multiplier = multiplier
        self.divider = divider

        self.stack = []

    def enter_number(self, number):
        '''push operand onto stack DKA'''

        self.stack.insert(0, number)

    def _do_calc(self, operator):
        '''execute chosen calculation DKA'''

        try:
            # result = operator.calc(self.stack[0], self.stack[1]) #DKA removed
            result = operator.calc(self.stack[1], self.stack[0])  # DKA added
        except IndexError:
            raise InsufficientOperands

        self.stack = [result]
        return result

    def add(self):
        '''add DKA'''

        return self._do_calc(self.adder)

    def subtract(self):
        '''subtract DKA'''

        return self._do_calc(self.subtracter)

    def multiply(self):
        '''multiply DKA'''

        return self._do_calc(self.multiplier)

    def divide(self):
        '''divide DKA'''

        return self._do_calc(self.divider)