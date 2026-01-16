n, m=map(int, input().split())
w=list(map(int, input().split()))
w.insert(0,0)
f=[[0]*(n+5) for i in range(m+5)]

for i in range(n+1):
    f[i][0]=1
for i in range(1,n+1):
    for j in range(1,m+1):
        if w[i]>j:
            f[i][j]=f[i-1][j]
        else:
            f[i][j]=f[i-1][j]+f[i-1][j-w[i]]

print(f[n][m])