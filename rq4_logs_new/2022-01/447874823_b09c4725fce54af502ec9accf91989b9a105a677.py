def solution(N, stages):
    failure = {}
    a = len(stages)
    for i in range(1,N+1):
        if a != 0:
            fail = stages.count(i)
            failure[i] = fail / a
            a -= fail
        else:
            failure[i] = 0
    return sorted(failure, key=lambda x: failure[x], reverse=True)

N = 4
stages = [4,4,4,4,4]
answer = solution(N, stages)
print(answer)