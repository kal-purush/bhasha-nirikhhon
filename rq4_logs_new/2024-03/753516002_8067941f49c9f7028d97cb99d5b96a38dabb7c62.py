n,m=map(int,input().split())
for i in range(n):
    for j in range(m):
        print(str((i*j)).ljust(3), end='')
    print()