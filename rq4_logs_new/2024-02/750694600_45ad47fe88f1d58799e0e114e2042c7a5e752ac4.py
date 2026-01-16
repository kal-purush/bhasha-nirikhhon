# N=int(input())
# cnt=0
# # min_cnt=N
# tes=N

# while tes!=1:
#     cnt+=1
#     if tes%8==0:
#         cnt-=1
#         while tes%2==0:
#             cnt+=1
#             tes=int(tes/2)
#     elif tes%3==0:
#         cnt-=1
#         while tes%3==0:
#             cnt+=1
#             tes=int(tes/3)
#     elif tes%3==1:
#         tes-=1
#     elif tes%2==1:
#         cnt+=1
#         tes-=1
#         tes=int(tes/2)
#     elif tes%2==0:
        
#         tes=int(tes/2)
#     else:
        
#         tes-=1
# # if min_cnt>cnt:
# #     min_cnt=cnt


# print(cnt)
#아무리 조건을 달아줘도 예외가 생긴다 -> dp로 풀어야 한다..
import sys
sys.stdin=open('1463_input.txt')
N = int(input())
dp=[0]*(N+1)
# N = int(input())
# dp=[0]*(N+1)

# def make1(n):
#     global dp
#     if dp[n] or n==1 :
#         return dp[n]
    
#     if n%3==0 and n%2==0:
#         dp[n]=min(make1(n//3), make1(n//2))+1
#     elif n%3==0:
#         dp[n]=min(make1(n-1), make1(n//3))+1
#     elif n%2==0:
#         dp[n]=min(make1(n-1), make1(n//2))+1
#     else :
#         dp[n]=make1(n-1)+1

#     return dp[n]
# print(make1(N))
# 재귀함수로 푸는 방법 > 48번째 줄에서 굳이 1뺀값과 비교 불필요(어차피 나눈값이 작음)
for i in range(2,N+1):
    if i%3==0 and i%2==0:
        dp[i]=min(dp[i-1],dp[i//3],dp[i//2])+1
    elif i%3==0:
        dp[i]=min(dp[i-1],dp[i//3])+1
    elif i%2==0:
        dp[i]=min(dp[i-1],dp[i//2])+1
    else:
        dp[i]=dp[i-1]+1
print(dp[N])