from pyformlang.cfg import CFG, Variable, Terminal, Production, Epsilon
from pyformlang.regular_expression import Regex
from typing import List, Set
from src.Utils import read_tokens
from src.CFGrammar import CFGrammar
import os
from itertools import groupby


class SyntaxAnalyzer:
    def __init__(self, name):
        self.cfg = SyntaxAnalyzer.read_grammar(name)
        self.eps = self.cfg.generate_epsilon()
        self.cnf = self.cfg.to_normal_form()

    @classmethod
    def read_grammar(cls, name):        
        id = 0

        terminals, variables, productions = set(), set(), set()
        start_symb = None

        with open(name, 'r') as file:
            productions_txt = file.readlines()

            for production_txt in productions_txt:
                head, _, *body_full = production_txt.strip().split()

                if start_symb is None:
                    start_symb = Variable(head)

                tmp_body = []
                bodies = [list(group) for k, group in groupby(body_full, lambda x: x == "|") if not k]

                for body in bodies:

                    is_regex = not any([True if '*' not in value else False for value in body])

                    if is_regex:
                        new_productions, new_variables, new_terminals, id = CFGrammar \
                                                                            .read_production_regex(head, Regex.from_python_regex(body[0]), id, False)
                    
                        productions |= new_productions
                        variables |= new_variables
                        terminals |= new_terminals
                    else:
                        body_cfg = []
                        for letter in body:
                            if letter == "epsilon":
                                body_cfg.append(Epsilon())
                            elif letter.isupper():
                                non_terminal = Variable(letter)
                                variables.add(non_terminal)
                                body_cfg.append(non_terminal)
                            else:
                                terminal = Terminal(letter)
                                terminals.add(terminal)
                                body_cfg.append(terminal)

                        productions.add(Production(Variable(head), body_cfg))

        cfg = CFG(variables, terminals, start_symb, productions)

        return cfg