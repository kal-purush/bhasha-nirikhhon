#!/usr/bin/python3

import tokenize as tok

def digit_to_roman(digit, unit):
    if unit == 1000:
        return digit * 'M'

    if unit == 100:
        if digit == 0:
            return ''
        elif digit in {1, 2, 3}:
            return digit * 'C'
        elif digit == 4:
            return 'CD'
        elif digit == 5:
            return 'D'
        elif digit == 6:
            return 'DC'
        elif digit == 7:
            return 'DCC'
        elif digit == 8:
            return 'DCCC'
        elif digit == 9:
            return 'CM'

    if unit == 10:
        if digit == 0:
            return ''
        elif digit == 9:
            return 'XC'
        elif digit == 8:
            return 'LXXX'
        elif digit == 7:
            return 'LXX'
        elif digit == 6:
            return 'LX'
        elif digit == 5:
            return 'L'
        elif digit == 4:
            return 'XL'
        elif digit in {3, 2, 1}:
            return digit * 'X'

    if unit == 1:
        if digit == 0:
            return ''
        elif digit == 9:
            return 'IX'
        elif digit == 8:
            return 'VIII'
        elif digit == 7:
            return 'VII'
        elif digit == 6:
            return 'VI'
        elif digit == 5:
            return 'V'
        elif digit == 4:
            return 'IV'
        elif digit in {3, 2, 1}:
            return digit * 'I'

def symbol_to_int(symbol):
    if symbol == 'M':
        return 1000
    elif symbol == 'D':
        return 500
    elif symbol == 'C':
        return 100
    elif symbol == 'L':
        return 50
    elif symbol == 'X':
        return 10
    elif symbol == 'V':
        return 5
    elif symbol == 'I':
        return 1

    raise RuntimeError("Invalid symbol")

class InvalidRomanNumeral(Exception):
    def __init(self, token):
        self.tok = token

    def token(self):
        return self.token

def token_to_int(tok):

    if tok == "CM":
        return 900

    if tok == "CD":
        return 400

    if tok == "XC":
        return 90

    if tok == "XL":
        return 40

    if tok == "IX":
        return 9

    if tok == "IV":
        return 4

    return sum(map(symbol_to_int, list(tok)))

class RomanNumber:
    def __init__(self, roman_numeral=None):
        self.intrep = 0
        self.strrep = ""
        self.cur_token = ""
        self.is_valid = False

        if not roman_numeral:
            return

        if type(roman_numeral) is str:
            self.fromString(roman_numeral)
        elif type(roman_numeral) is int:
            self.fromInt(roman_numeral)
        else:
            raise RuntimeError("Cannot initialize from " + type(roman_numeral))


    def __repr__(self):
        return self.strrep

    def asInt(self):
        return self.intrep

    def fromString(self, s):
        self.intrep = 0
        self.strrep = s.upper()
        self.cur_token = ""
        self.is_valid = False
        self.token_gen = tok.tokenize(self.strrep)
        self.__thou()
        if not self.is_valid:
            raise InvalidRomanNumeral(self.cur_token)

    def fromInt(self, n):
        self.intrep = n
        self.strrep = ""

        digit = n % 10
        self.strrep = digit_to_roman(digit, 1)

        n /= 10
        digit = n % 10
        self.strrep = digit_to_roman(digit, 10) + self.strrep

        n /= 10
        digit = n % 10
        self.strrep = digit_to_roman(digit, 100) + self.strrep

        n /= 10
        self.strrep = digit_to_roman(n, 1000) + self.strrep

    def __thou(self):
        try:
            self.cur_token = self.token_gen.next()
            if self.cur_token == 'M':
                self.intrep += token_to_int(self.cur_token)
                self.__thou()
            else:
                self.__hund()
        except StopIteration:
            self.is_valid = True
            return

    def __hund(self):
        try:
            if self.cur_token in {
                    "CM",
                    "DCCC",
                    "DCC",
                    "DC",
                    "D",
                    "CD",
                    "CCC",
                    "CC",
                    "C"}:
                self.intrep += token_to_int(self.cur_token)
                self.cur_token = self.token_gen.next()
                self.__tens()
            else:
                self.__tens()
        except StopIteration:
            self.is_valid = True
            return

    def __tens(self):
        try:
            if self.cur_token in {
                    "XC",
                    "LXXX",
                    "LXX",
                    "LX",
                    "L",
                    "XL",
                    "XXX",
                    "XX",
                    "X"}:
                self.intrep += token_to_int(self.cur_token)
                self.cur_token = self.token_gen.next()
                self.__ones()
            else:
                self.__ones()
        except StopIteration:
            self.is_valid = True
            return


    def __ones(self):
        try:
            if self.cur_token in {
                    "VIII",
                    "VII",
                    "VI",
                    "V",
                    "IV",
                    "III",
                    "II",
                    "I"}:
                self.intrep += token_to_int(self.cur_token)
                self.cur_token = self.token_gen.next()
            else:
                raise InvalidRomanNumeral(self.cur_token)
        except StopIteration:
            self.is_valid = True
            return