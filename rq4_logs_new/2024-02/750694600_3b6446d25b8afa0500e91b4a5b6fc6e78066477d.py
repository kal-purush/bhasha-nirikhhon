'''
백준 2304 창고 다각형

문제 해석
입력 - n(기둥의 개수), l(각 기둥의 왼쪽 면의 위치), h(각 기둥의 높이)
출력 - 가장 작은 창고 다각형의 면적

지붕은 수평 부분과 수직부분으로 구성 되며, 모두 연결된다.
지붕의 수평 부분은 반드시 어떤 기둥의 윗면과 닿는다
지붕의 수직 부분은 반드시 어떤 기둥의 옆면과 닿는다.
지붕의 가장자리는 땅에 닿는다.
지붕은 오목하게 들어간 부분이 없다.


문제 풀이 절차
아이디어
가장 큰 기준점 찾기
기준점 까지 큰 거 만나면 올라가며 더해줌
왼쪽은 오른쪽과 반대로.
'''
n = int(input())
lst = [0]*1001
mxindex = 0
mx = 0
ans = 0

for _ in range(n):
    l, h = map(int,input().split())
    lst[l] = h
    
    if mx < h:
        mxindex = l

for i in range(mxindex+1):
    mx = max(mx, lst[i])
    ans += mx

mx = 0

for i in range(1000, mxindex, -1):
    mx = max(mx, lst[i])
    ans += mx

print(ans)