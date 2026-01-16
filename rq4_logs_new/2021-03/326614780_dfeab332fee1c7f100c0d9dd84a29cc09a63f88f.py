import sys
import heapq

heap = {}
keys = []
for _ in range(int(sys.stdin.readline())):
    x = int(sys.stdin.readline())
    if x:
        absx = abs(x)
        if heap.get(absx):
            heapq.heappush(heap[absx], x)
        else:
            heap[absx] = [x]
            heapq.heappush(keys, absx)
    else:
        try:
            h = heapq.heappop(keys)
            if len(heap[h]) > 1:
                print(heapq.heappop(heap[h]))
                heapq.heappush(keys, h)
            else:
                print(heap.pop(h)[0])
        except IndexError:
            print(0)