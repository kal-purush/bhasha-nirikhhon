from tests import OSmiPyTestCase

from osmipy import smiles, lexer
from osmipy.tokens import Token as Tok


class LexerTestCase(OSmiPyTestCase):

    def test_lexer(self):
        """Test that the lexer gives the good tokens"""

        tests = [
            ('cccccc', [Tok(smiles.ATOM, 'c')] * 6),
            ('C1=CC=CC=C1', [
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.DIGIT, 1),
                Tok(smiles.BOND, '='),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.BOND, '='),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.BOND, '='),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.DIGIT, 1),
            ]),
            ('N[C@](Br)(O)C', [
                Tok(smiles.ATOM, 'N'),
                Tok(smiles.LSPAR, '['),
                Tok(smiles.ATOM, 'C'),
                Tok(smiles.AT, '@'),
                Tok(smiles.RSPAR, ']'),
                Tok(smiles.LPAR, '('),
                Tok(smiles.ATOM, 'Br'),
                Tok(smiles.RPAR, ')'),
                Tok(smiles.LPAR, '('),
                Tok(smiles.ATOM, 'O'),
                Tok(smiles.RPAR, ')'),
                Tok(smiles.ATOM, 'C'),
            ])
        ]

        for s, r in tests:
            smiles_lexer = lexer.Lexer(s)
            lexed = [i for i in smiles_lexer.tokenize()]
            self.assertEqual(len(lexed) - 1, len(r))
            for i, token in enumerate(r):
                self.assertEqual(token.type, lexed[i].type, msg='{} of {}'.format(i, s))
                self.assertEqual(token.value, lexed[i].value, msg='{} of {}'.format(i, s))

            self.assertEqual(lexed[-1].type, smiles.EOF)