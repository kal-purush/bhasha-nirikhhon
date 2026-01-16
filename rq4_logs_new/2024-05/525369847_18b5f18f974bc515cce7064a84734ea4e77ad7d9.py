# A問題
N, X, Y, Z = map(int, input().split())

if X > Y:
    for i in range(X, Y - 1, -1):
        if i == Z:
            print('Yes')
            exit()
    else:
        print('No')
        exit()
else:
    for i in range(X, Y + 1, 1):
        if i == Z:
            print('Yes')
            exit()
    else:
        print('No')
        exit()

# B問題
S = list(input())
T = list(input())

result = []
i = 0

for s in S:
    for t in range(i, len(T)):
        if s == T[t]:
            result.append(t + 1)
            i = t + 1
            break

print(' '.join(map(str, result)))

# C問題
N = int(input())

head = 0
Diff = 0
result = 0

for _ in range(N):
    A, B = map(int, input().split())
    head += A
    if (B - A) > Diff:
        Diff = (B - A)

result += head + Diff

print(result)

# D問題