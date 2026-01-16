import heapq 
def ls3(A, B, C):
    pq = []
    res = ""
    for k, v in ('a', A), ('b', B), ('c', C):
        heapq.heappush(pq, (-v, k))
    preV, preK = 0, ''
    while pq:
        v, k = heapq.heappop(pq)
        print("popped out", v, " ", k) 
        if preV:
            heapq.heappush(pq, (preV, preK))
            print("pushed in", preV, " ", preK) 
            preV, preK = 0, ''
        if res[-2:] == k * 2:
            preV, preK = v, k
            print("Changed to ", v, " ", k)
        else:
            res += k
            print("Added string ", k) 
            if v != -1:
                heapq.heappush(pq, (v + 1, k))
                print("pushed in", v + 1, " ", k)
    return res


A, B, C = 1, 1, 6
r = ls3(A, B, C) 
print("result is ", r) 
