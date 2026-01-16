
def opposite_point(a, b, p):
    b = -b
    while b < 0:
        b += p
    return [a, b]


if __name__ == "__main__":
    print(opposite_point(3, 2, 7))
    print(opposite_point(1, 4, 5))