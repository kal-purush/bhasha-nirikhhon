def sum(a, b):
    if not type(a) is int or not type(b) is int:
        raise TypeError('!')
    elif a <= 0 or b <= 0:
            raise ValueError("!!")
    return a + b