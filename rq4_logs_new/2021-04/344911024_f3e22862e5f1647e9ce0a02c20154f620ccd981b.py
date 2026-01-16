from multiplication_table.process_math.Graph import Graph
from scipy.sparse import isspmatrix


def test_table_number():
    assert (type(Graph(2, 10).N) == float)


def test_modulo():
    assert (type(Graph(2, 10).mod) == int)


def test_sparse():
    assert isspmatrix(Graph(2, 10).M)


def test_sparse_size():
    assert (Graph(2, 15).M.shape == (1500, 1500))


def test_modulo_result1():
    assert (type(Graph(2, 10).modulo_result(5)) == float)


def test_modulo_result2():
    assert (Graph(2, 10).modulo_result(5) == 2.0 * 5 % 10)