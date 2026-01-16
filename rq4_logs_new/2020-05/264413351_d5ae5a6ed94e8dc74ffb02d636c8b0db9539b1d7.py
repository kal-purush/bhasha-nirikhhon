from prime import is_prime
import pytest

def test_prime_valid_values():
    assert is_prime(10) == False
    assert is_prime(100) == False
    assert is_prime(11) == True
    assert is_prime(101) == True

def test_prime_invalid_values():
    with pytest.raises(TypeError) as err:
        is_prime("1")

    with pytest.raises(TypeError) as err:
        is_prime(10.1)

    with pytest.raises(TypeError) as err:
        is_prime([1, 2, 3])

    with pytest.raises(TypeError) as err:
        is_prime((1, 2, 3))

    with pytest.raises(TypeError) as err:
        is_prime(None)