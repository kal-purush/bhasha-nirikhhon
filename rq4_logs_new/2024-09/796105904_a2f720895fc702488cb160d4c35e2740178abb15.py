from src.ac_editor import is_valid_vim

def test_is_valid_vim():
    # Command : Regex
    tests = {
        "h" : "[0-9]*h"
    }
    passed = True
    for test in tests:
        valid, regex = is_valid_vim(test) 
        regex_answer = tests[test]
        passed &= (valid == True and regex == regex_answer)

    assert(passed == True)