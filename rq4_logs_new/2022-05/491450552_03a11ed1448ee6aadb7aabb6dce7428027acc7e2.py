a = int(input())
b = int(input())

prime = []
for num in range(a, b+1):
    if num == 1:
        pass
    elif num == 2:
        prime.append(num)
    else:
        for i in range(2, num):
            if num%i == 0:
                break
            elif num%i !=0 and num == i+1:
                prime.append(num)

if len(prime)>=1:
    print(sum(prime))
    print(min(prime))
else:
    print(-1)
def solution(nums):
    n = len(nums)//2
    l = list(set(nums))
    if len(l)>n:
        return n
    else:
        return len(l)


# 어이없음
# def solution(ls):
#     return min(len(ls)/2, len(set(ls)))

# 시간 초과
# from itertools import combinations
# def solution(nums):
#     answer = 0
#     l = list(map(list, combinations(nums, len(nums)//2)))
#     for i in l:
#         if answer < len(set(i)):
#             answer = len(set(i))
#     return answer

nums = [3,3,3,2,2,4]
print(solution(nums))


