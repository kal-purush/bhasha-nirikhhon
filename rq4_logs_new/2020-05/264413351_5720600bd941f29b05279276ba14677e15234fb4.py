from pyproblems.palindrome import is_palindrome
import pytest

def test_palindrome_valid():
    assert is_palindrome('111')
    assert is_palindrome('malayalam')
    assert is_palindrome('liril')

def test_palindrome_invalid():
    assert not is_palindrome("something")
    assert not is_palindrome("everything")
    assert not is_palindrome("nothing")

def test_palindrome_invalid_types():
    invalid = [("123", "456", "789"), (6, "1", "3"), {1: "123"}, None, {123, 123, 123}, True, False]
    
    for value in invalid:
        with pytest.raises(TypeError):
            is_palindrome(value)