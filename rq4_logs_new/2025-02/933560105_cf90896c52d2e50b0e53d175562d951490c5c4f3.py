from collections import deque
import sys

input = sys.stdin.readline
t = int(input())

dy = [1, -1, 0, 0]
dx = [0, 0, 1, -1]

for _ in range(t):
    m, n, k = map(int, input().split())
    field = [[0 for _ in range(m)] for _ in range(n)]

    for _ in range(k):
        x, y = map(int, input().split())
        field[y][x] = 1

    count = 0

    for i in range(n):
        for j in range(m):
            if field[i][j] == 1:
                count += 1
                queue = deque()
                queue.append((j, i))
                while queue:
                    x, y = queue.pop()
                    field[y][x] = 0
                    for j in range(4):
                        _x = x + dx[j]
                        _y = y + dy[j]
                        if (0 <= _x < m) and (0 <= _y < n) and (field[_y][_x] == 1):
                            queue.append((_x, _y))
                            
    print(count)