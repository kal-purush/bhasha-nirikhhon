N = int(input())
switch = list(map(int,input().split()))
C = int(input())
for _ in range(C):
    S, G = map(int, input().split())
    if S == 1:
        for i in range(len(switch)):
            if (i+1) % G == 0:
                switch[i] = 1 - switch[i]
    else:
        i = G - 2
        j = G
        switch[G-1] = 1 - switch[G-1]
        while i > -1 and j < N and switch[i] == switch[j]:
            switch[i] = 1 - switch[i]
            switch[j] = 1 - switch[j]
            i -= 1
            j += 1
for i in range(1, N+1):
    print(switch[i-1], end = " ")
    if i % 20 == 0 :
        print()