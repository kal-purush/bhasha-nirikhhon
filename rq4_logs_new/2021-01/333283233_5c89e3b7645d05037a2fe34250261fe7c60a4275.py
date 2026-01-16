from example import*

def test_add3():
  assert add3(1, 2, 3) == 6
  assert add3(5, 5, 5,) == 15
  assert add3("EA", "Z", "Y") == "EAZY"

def test_numpyaround():
  assert numpyaround(1.222, 1) == 1.2