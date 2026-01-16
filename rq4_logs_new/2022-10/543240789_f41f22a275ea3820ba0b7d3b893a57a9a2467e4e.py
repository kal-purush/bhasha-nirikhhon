# def mis_cost(l):
#     start = 0
#     if len(l)==1:
#         return l[0]
#     elif len(l)==2:
#         if l[0]>l[1]:
#             return l[1]
#         else:
#             return l[0]
#     elif len(l)==3:
#         if l[0]+l[2]>l[1]:
#             return l[1]
#         else:
#             return l[0]+l[2]
#     else:
#         count = 0
#         if l[0]>l[1]:
#             start = 1
#             count += l[1]
#         else:
#             start = 0
#             count += l[0]
#         #for i in range(start, len(l)-2 ):
#         while (start<len(l)):
#             print(l[start],start,len(l))
#             # if start==len(l)-1:
#             #     count += l[start]
#             #     start += 1
#             if l[start+1]==l[start+2]:
#                 start = start+2
#                 count += l[start]
#             elif l[start+1]>l[start+2]:
#                 start = start+2
#                 count += l[start]
#             else:
#                 start = start+1
#                 count += l[start]
#         return count

def mis_cost(l):
    if not l:
        return 0
    
    dp = [0] * len(l)
    dp[0] = l[0]
    if len(l)>=2:
        dp[1]=l[1]
    
    for i in range(2,len(l)):
        dp[i]=l[i]+min(dp[i-1],dp[i-2])
        
    return min(dp[-1],dp[-2])