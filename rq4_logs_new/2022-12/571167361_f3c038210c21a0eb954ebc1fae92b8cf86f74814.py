import less5_1

def test_6_simv():
    assert less5_1.func('spqlwekr', 0) == True

def test_check_password():
    assert less5_1.func('pAssWoRd', 0) == False

def test_num():
    assert less5_1.func('asoqk1OklamA', 0) == True

def test_only_num():
    assert less5_1.func('123212553123', 0) == False