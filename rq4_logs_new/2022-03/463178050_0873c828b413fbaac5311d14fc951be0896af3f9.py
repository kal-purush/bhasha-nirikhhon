# -----------------------------------------------------------------------------
# calc.py
# -----------------------------------------------------------------------------

from ejemplo import tokens
from sly import Lexer, Parser

class CalcLexer(Lexer):
    tokens = { KEY, NUMERIC, EXTRA, HOUR, QUIT, ENTRE, EQUAL, LEFT, RIGHT }
    ignore = ' \t'

    # Tokens
    KEY = r'[a-zA-Z_][a-zA-Z0-9_]*'
    NUMERIC = r'\d+'

    # Special symbols
    EXTRA = r'\+'
    QUIT = r'-'
    HOUR = r'\*'
    ENTRE = r'/'
    EQUAL = r'='
    LEFT = r'\('
    RIGHT = r'\)'

    # Ignored pattern
    ignore_newline = r'\n+'

    # Extra action for newlines
    def ignore_newline(self, t):
        self.lineno += t.value.count('\n')

    def error(self, t):
        print("Illegal character '%s'" % t.value[0])
        self.index += 1

class CalcParser(Parser):
    tokens = CalcLexer.tokens

    precedence = (
        ('left', EXTRA, QUIT),
        ('left', HOUR, ENTRE),
        ('right', UQUIT),
        )

    def __init__(self):
        self.names = { }

    @_('KEY EQUAL expr')
    def statement(self, p):
        self.names[p.KEY] = p.expr

    @_('expr')
    def statement(self, p):
        print(p.expr)

    @_('expr EXTRA expr')
    def expr(self, p):
        return p.expr0 + p.expr1

    @_('expr QUIT expr')
    def expr(self, p):
        return p.expr0 - p.expr1

    @_('expr HOUR expr')
    def expr(self, p):
        return p.expr0 * p.expr1

    @_('expr ENTRE expr')
    def expr(self, p):
        return p.expr0 / p.expr1

    @_('QUIT expr %prec UQUIT')
    def expr(self, p):
        return -p.expr

    @_('LEFT expr RIGHT')
    def expr(self, p):
        return p.expr

    @_('NUMERIC')
    def expr(self, p):
        return int(p.NUMERIC)

    @_('KEY')
    def expr(self, p):
        try:
            return self.names[p.KEY]
        except LookupError:
            print(f'Undefined name {p.KEY!r}')
            return 0

if __name__ == '__main__':
    lexer = CalcLexer()
    parser = CalcParser()
    while True:
        try:
            text = input('calc > ')
        except EOFError:
            break
        if text:
            parser.parse(lexer.tokenize(text))