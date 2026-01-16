import pytest
from pyproblems.matrix_diagonal_sum import diagonal_sum

def test_diagonal_sum_4x4():
    lis = [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]]
    assert diagonal_sum(lis) == 20

def test_diagonal_sum_3x3():
    lis = [[2,3,4],[1,3,4],[1,2,4]]
    assert diagonal_sum(lis) == 17

def test_notasquare_mat():
    lis = [[2,3,4],[1,3,4],[1,2,4],[4,5,7]]
    with pytest.raises(IndexError) as err:
        diagonal_sum(lis)
        assert err.value.args[0] == "Invalid Matrix Size, Enter a square matrix"

def test_matrix_typeerr():
    lis = [[2,'jothi',4],[1,3,4],[1,2,4]]
    with pytest.raises(TypeError) as err:
        diagonal_sum(lis)
        assert err.value.args[0] ==f"Invalid type of entry{type(lis[0][1])}"

def test_tuple_matrix():
    lis = ([2,3,4],[1,3,4],[1,2,4])
    with pytest.raises(TypeError) as err:
        diagonal_sum(lis)
        assert err.value.args[0] =="Enter the matrix as a list of lists"

def test_tuple_notasquare_mat():
    lis = ([2,3,4],[1,3,4],[1,2,4],[2,'jothi',4])
    with pytest.raises(TypeError) as err:
        diagonal_sum(lis)
        assert err.value.args[0] =="Enter the matrix as a list of lists"

def test_listoftuple_notasquare_mat():
    lis = [[2,3,4],[1,3,4],[1,2,4],(2,'jothi',4)]
    with pytest.raises(TypeError) as err:
        diagonal_sum(lis)
        assert err.value.args[0] == "The elements in the list, should also be a list"