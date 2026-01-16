from osmipy.tokens import *


class LexerException(Exception):
    def __init__(self, position, msg):
        super().__init__('lexer error at position {}: {}'.format(position, msg))
        self.position = position
        self.message = msg


class Lexer:
    """Lexer
    """

    def __init__(self, input_):
        self.input = input_
        self.pos = 0
        self.current_char = None
        if len(self.input) > 0:
            self.current_char = self.input[self.pos]

    def next(self):
        """Go to the next character
        """
        self.pos += 1
        self.current_char = None if self.pos >= len(self.input) else self.input[self.pos]

    def atom(self):
        """Consume and return an atomic symbol from the input.

        :return: str
        """

        result = self.current_char
        pos = self.pos
        self.next()

        if self.current_char is not None and self.current_char.isalpha():
            nresult = result + self.current_char
            if nresult in TOT_SYMBOLS:
                self.next()
                return nresult

        if result in TOT_SYMBOLS:
            return result
        else:
            raise LexerException(pos, '{} is not a valid atomic symbol'.format(result))

    def tokenize(self):
        """Tokenize the input
        """

        while self.current_char is not None:
            pos = self.pos
            if self.current_char in SYMBOLS_TR:
                yield Token(SYMBOLS_TR[self.current_char], self.current_char, pos)
                self.next()
                continue

            elif self.current_char.isdigit():
                yield Token(DIGIT, int(self.current_char), pos)
                self.next()
                continue

            elif self.current_char.isalpha():
                yield Token(ATOM, self.atom(), pos)
                continue

            else:
                raise LexerException(pos, 'unknown symbol {}'.format(self.current_char))

        yield Token(EOF, None, self.pos)