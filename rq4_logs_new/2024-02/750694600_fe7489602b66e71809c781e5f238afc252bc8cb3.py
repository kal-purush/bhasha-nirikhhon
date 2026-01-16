def flip(n) :
    if n == 0:
        return 1
    if n == 1:
        return 0 

switch_n = int(input())
switch = list(map(int,input().split()))

tc = int(input())
for _ in range(tc) : 
    g, n = map(int,input().split())
    if g == 1 : #남학생
        for i in range(switch_n):
            if (i+1)%n == 0 :
                switch[i] = flip(switch[i])
   
    else : #여학생
        i = 0
        while 0<=n-1-i and n-1+i<switch_n and switch[n-1-i] == switch[n-1+i] :
            switch[n-1-i] = flip(switch[n-1-i])
            if i != 0:
                switch[n-1+i] = flip(switch[n-1+i])
            i+=1

for j in range(switch_n):
    if j!=0 and j%20 == 0:
        print()
    print(switch[j],end = ' ')
    