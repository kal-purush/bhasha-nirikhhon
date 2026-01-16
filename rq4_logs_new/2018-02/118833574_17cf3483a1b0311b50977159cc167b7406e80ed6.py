
for i in range(8):
    x = ""
    if i % 2 == 0:
        for m in range(8):
            if m % 2 == 0:
                x += "*"
            else:
                x += " "
    else:
        for m in range(8):
            if m % 2 == 0:
                x += " "
            else:
                x += "*"
    print(x)
    