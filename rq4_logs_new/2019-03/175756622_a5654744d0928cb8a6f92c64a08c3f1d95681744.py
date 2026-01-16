def multiples_of_five():
    output = []
    for i in range(1,50 +1):
        if i % 5 != 0:
            continue
        output.append(i)
    return output