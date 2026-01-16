a, b = 1, 1
while a < 50:
    print(a, end=",")
    a, b = b, a + b
else:
    print("\n不合法", a)