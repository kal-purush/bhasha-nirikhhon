#!/usr/bin/python3

import tokenize as tok

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
    def __init(self, token, position):
        self.tok = token
        self.pos = position

    def token(self):
        return self.token

    def position(self):
        return self.pos

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

class Parser:
    def __init__(self):
        self.intrep = 0
        self.strrep = ""
        self.cur_token = ""
        self.is_valid = False
        self.cur_pos = 0

    def parse(self, roman_numeral):
        self.intrep = 0
        self.strrep = roman_numeral.upper()
        self.cur_token = ""
        self.is_valid = False
        self.cur_pos = 0

        if not roman_numeral:
            return

        self.token_gen = tok.tokenize(self.strrep)
        self.__thou()

        if not self.is_valid:
            raise InvalidRomanNumeral(self.cur_token,
                                      self.cur_pos - len(self.cur_token))

        return self.intrep

    def __thou(self):
        try:
            self.cur_token = self.token_gen.next()
            self.cur_pos += len(self.cur_token)
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
                self.cur_pos += len(self.cur_token)
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
                self.cur_pos += len(self.cur_token)
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
                self.cur_pos += len(self.cur_token)
            else:
                raise InvalidRomanNumeral(self.cur_token,
                                          self.cur_pos - len(self.cur_token))
        except StopIteration:
            self.is_valid = True
            return