from DictInterface import *
import pytest

def test_has_7_unique_letters_returns_bool():
    word = "Bromine"
    result = has_7_unique_letters(word);
    assert isinstance(result, bool)

def test_has_7_unique_letters_with_valid_input():
    # Test with valid words that have 7 unique letters
    assert has_7_unique_letters("abcdefg") is True
    assert has_7_unique_letters("bromines") is True
    assert has_7_unique_letters("pythoni") is True

def test_has_7_unique_letters_with_invalid_input():
    # Test with words that do not have 7 unique letters
    assert has_7_unique_letters("hello") is False  # 4 unique letter
    assert has_7_unique_letters("abc") is False  # Less than 7 letters

def test_has_7_unique_letters_with_empty_input():
    # Test with an empty string
    assert has_7_unique_letters("") is False

def test_has_7_unique_letters_with_whitespace_input():
    # Test with a word containing whitespace
    assert has_7_unique_letters("        ") is False

def test_randomWord_returns_string():
    word = randomWord();
    assert isinstance(word, str)

def test_randomWord_returns_7_unique_letter_word():
    word = randomWord();
    assert len(set(word)) >= 7

def test_isValid_returns_bool():
    result = isValid("lump")
    assert isinstance(result, bool)

def test_findValid_returns_set():
    result = findValid('r','bromine' )
    assert isinstance(result, set)