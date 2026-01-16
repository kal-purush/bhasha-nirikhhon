import os
import pytest
from itertools import chain
from src.Graph import Graph
from src.CFGrammar import CFGrammar
from src.RFA import RFA
from src.Utils import read_graph, parse_pairs
import src.cfpq as cfpq

DATA_PATH = os.path.join(os.getcwd(), 'tests/data/cfpq')

grammars_name = ["grammar_1_parentheses.txt", "grammar_2_palindrome.txt", "grammar_3.txt"]

graphs_name = ["graph_empty.txt", "graph_1.txt", "graph_2.txt", "graph_3.txt"]

res_name = ["res_1.txt", "res_2.txt", "res_3.txt"]


@pytest.fixture(scope='function', params=list(chain(*[
    [
        (grammars_name[0], graphs_name[0], algo_name, []),
        (grammars_name[1], graphs_name[2], algo_name, res_name[1]),
        (grammars_name[2], graphs_name[3], algo_name, res_name[2])
    ]
    for algo_name in [f'cfpq_{name}' for name in ['hellings', 'matrices', 'tensor']]
])))
def init(request):
    grammar_name, graph_name, algo_name, res_name = request.param

    grammar = CFGrammar(os.path.join(DATA_PATH, grammar_name))

    graph = Graph(read_graph(os.path.join(DATA_PATH, graph_name)))

    expected = set(parse_pairs(os.path.join(DATA_PATH, res_name))) if res_name != [] else set([])

    return {
        'grammar': grammar,
        'graph': graph,
        'algo': algo_name,
        'expected': expected
    }


def test_cfpq(init):
    grammar, graph, algo_name, expected = init['grammar'], init['graph'], init['algo'], init['expected']
    algo = getattr(cfpq, algo_name)
    
    assert set(algo(graph, grammar)) == expected