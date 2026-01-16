nm = list(map(int, input().split()))
N = nm[0]; M = nm[1]
ary=[[0]];dp = [[0]*(M+1)]
for i in range(1, N+1):
    dp.append([0]*(M+1))
    ary.append(list(map(int, input().split())))
    ary[i].insert(0, 0)
    for j in range(1, M+1):
        dp[i][j] = dp[i-1][j] + dp[i][j-1] - dp[i-1][j-1] + ary[i][j]

K = int(input())
for k in range(K):
    ijxy = list(map(int, input().split()))
    i = ijxy[0]; j = ijxy[1]; x = ijxy[2]; y = ijxy[3]
    s = dp[x][y] - dp[x][j-1] -dp[i-1][y] + dp[i-1][j-1]
    print(s)