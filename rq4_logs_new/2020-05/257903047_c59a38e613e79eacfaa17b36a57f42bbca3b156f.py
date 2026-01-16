# [파이썬 | 알고스팟 | MAXSUM] 최대 연속 부분합 찾기
import sys
read = sys.stdin.readline
MINVAL = sys.maxsize * -1

def inefficient(N, arr):
    ret = MINVAL
    for i in range(N):
        for j in range(i, N):
            ret = max(ret, sum(arr[i:j+1]))
    return ret

def divideAndConquer(N, arr, left, right):
    if left == right:
        return arr[left]
    mid = (left + right) // 2
    
    leftval, rightval = MINVAL, MINVAL
    lo, hi = mid, mid+1
    while lo>=left:
        leftval = max(leftval, sum(arr[lo:mid+1]))
        lo -= 1

    while hi<=right:
        rightval = max(rightval, sum(arr[mid+1:hi+1]))
        hi += 1
    
    ret = leftval + rightval
    #print(divideAndConquer(N, arr, left, mid), divideAndConquer(N, arr, mid+1, right), ret)
    return max(divideAndConquer(N, arr, left, mid), divideAndConquer(N, arr, mid+1, right), ret)

def dynamic(N, arr):
    dp = [0 for _ in range(N)]
    dp[0] = arr[0]
    for i in range(1, N):
        dp[i] = max(dp[i-1] + arr[i], arr[i])
    return max(dp)

for t in range(int(read())):
    N = int(read())
    arr = list(map(int, read().split()))
    ans = dynamic(N, arr)
    print(ans)