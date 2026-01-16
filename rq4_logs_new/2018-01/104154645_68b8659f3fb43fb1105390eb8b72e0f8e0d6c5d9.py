n = int(input())

for i in range(1, n + 1):
    print(" " * (n - i) + "*" * i)

for k in range(1, n):
    print(" " * k + "*" * (n - k))