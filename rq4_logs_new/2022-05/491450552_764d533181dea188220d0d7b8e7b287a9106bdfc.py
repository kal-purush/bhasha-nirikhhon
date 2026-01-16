n = int(input())

if n % 5 == 0:
    print(n//5)
else:
    p = 0
    while n > 0:
        n -= 3
        p += 1
        if n % 5 == 0:
            p += n // 5
            print(p)
            break
        elif n == 1 or n == 2:
            print(-1)
            break
        elif n == 0:
            print(p)
            break
def solution(n, lost, reserve):

    reserve_n = list(set(reserve) - set(lost))
    lost_n = list(set(lost) - set(reserve))

    answer = n - len(lost_n)

    for i in lost_n:

        if i-1 in reserve_n:
            answer += 1
            reserve_n.remove(i-1)

        elif i+1 in reserve_n:
            answer += 1
            reserve_n.remove(i+1)

    return answer

# def solution(n, lost, reserve):

#     answer1 = n - len(lost)
#     answer2 = n - len(lost)

#     for i in lost:
        
#         if i-1 in reserve:
#             reserve.remove(i-1)
#             answer1 += 1

#         elif i+1 in reserve:
#             reserve.remove(i+1)
#             answer1 += 1

#     for i in lost:
        
#         if i+1 in reserve:
#             reserve.remove(i+1)
#             answer2 += 1

#         elif i-1 in reserve:
#             reserve.remove(i-1)
#             answer2 += 1

#     return max(answer1, answer2)