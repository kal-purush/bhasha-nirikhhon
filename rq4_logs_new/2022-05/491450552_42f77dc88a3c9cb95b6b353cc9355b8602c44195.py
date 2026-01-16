str = input()
time = 0
for i in str:
    if i in ('A', 'B', 'C'):
        time += 3
    elif i in ('D', 'E', 'F'):
        time += 4
    elif i in ('G', 'H', 'I'):
        time += 5
    elif i in ('J', 'K', 'L'):
        time += 6
    elif i in ('M', 'N', 'O'):
        time += 7
    elif i in ('P', 'Q', 'R', 'S'):
        time += 8
    elif i in ('T', 'U', 'V'):
        time += 9
    elif i in ('W', 'X', 'Y', 'Z'):
        time += 10
print(time)
from itertools import combinations

def solution(nums):
    pick3 = list(combinations(nums, 3))
    answer = len(pick3)
    for i in range(len(pick3)):
        sum = pick3[i][0] + pick3[i][1] + pick3[i][2]
        for j in range(2, sum):
            if sum%j == 0:
                answer -= 1
                break 
    return answer
    
nums = [1, 2, 7, 6, 4]
print(solution(nums))