twos = [2**i for i in range(1, 100)]

def deli(n):
    res = set()
    for i in range(2, int(n ** .5)):
        if n % i == 0:
            res |= {i, n // i}
    if sum(1 for i in res if i in twos) >= 20:
        if sum(i for i in res if i not in twos) == 0:
            return '0'
        return sum(i for i in res if i not in twos)
    return 0

count = 0
for i in range(10**6, 10**20):
    U = deli(i)
    if U:
        count += 1
        print(i, U)
        if count == 5:
            break