from spaceman import *

def test_is_word_guessed():
    assert is_word_guessed("shoe", ["a","e","q","c","l"]) == False
    assert is_word_guessed("brave", ["y","h","a","o","e","b","v","r"]) == True
    assert is_word_guessed("door", ["o","p","r","d"]) == True

def test_get_guessed_word():
    assert get_guessed_word("knock", ["k","o","p","a"]) == "k_o_k"
    assert get_guessed_word("server", ["a","e","b","s"]) == "se__e_"
    assert get_guessed_word("collect", ["r","o","l","a","h","c"]) == "coll_c_"

def test_is_guess_in_word():
    assert is_guess_in_word("e", "meteor") == True
    assert is_guess_in_word("t", "candid") == False
    assert is_guess_in_word("b", "tour") == False

def main():
    test_is_word_guessed()
    test_get_guessed_word()
    test_is_word_guessed()

if __name__ == "__main__":
    main()