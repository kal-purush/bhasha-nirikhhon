'''
1 2 4 4
2 3 5 7
3 1 6 5
7 3 8 6
=> 26
'''
# A = list(map(int, input().split()))
# B = list(map(int, input().split()))
# C = list(map(int, input().split()))
# D = list(map(int, input().split()))
A, B, C, D = [list(map(int, input().split())) for _ in range(4)]

def recSum(x):
    result = (x[3]-x[1])*(x[2]-x[0])
    return result

def intersection(x,y):
    compX = [x[0], x[2], y[0], y[2]]
    compX.sort()
    compY = [x[1], x[3], y[1], y[3]]
    compY.sort()
    if x[0] < y[0] and x[2]<y[0] :
        return 0
    else :
        result = (compX[2]-compX[1])*(compY[2]-compY[1])
        return result

def realSum(a, b, c, d):
    temp = recSum(a)+recSum(b)+recSum(c)+recSum(d)
    temp -= intersection(a, b)+intersection(a,c)+intersection(a,d)+intersection(b,c)+intersection(b,d)+intersection(c,d)
    netX = [a[0], a[2], b[0], b[2], c[0], c[2], d[0], d[2]]
    netY = [a[1], a[3], b[1], b[3], c[1], c[3], d[1], d[3]]
    netX.sort()
    netY.sort()
    temp += (netX[4] - netX[3]) * (netY[4] - netY[3])
    return temp


print(realSum(A,B,C,D))