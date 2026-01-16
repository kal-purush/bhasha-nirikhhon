def find_number(y, a = 4):
    y = abs(y)
    y = str(y)
    if len(y) < a:
        return -1

    return int(y[a-1])

