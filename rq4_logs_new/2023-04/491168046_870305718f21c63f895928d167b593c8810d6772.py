# https://codeforces.com/gym/440884/problem/D

from collections import deque

t = int(input())
directions = [(0, 1), (1, 0), (0, -1), (-1, 0),
              (1, -1), (-1, 1), (1, 1), (-1, -1)]


def inbound(rx, cx, n):
    return 0 <= rx < 2 and 0 <= cx < n

def bfs(grid, n):
    queue = deque()
    queue.append((0, 0))
    visited = set()
    visited.add((0, 0))

    while queue:
        rx, cx = queue.popleft()

        for dr, dc in directions:
            nr, nc = rx + dr, cx + dc

            if inbound(nr, nc, n) and (nr, nc) not in visited:
                if nr == 1 and nc == n - 1:
                    return True
                if grid[nr][nc] == '0':
                    queue.append((nr, nc))
                    visited.add((nr, nc))
    return False

for _ in range(t):
    grid = []
    n = int(input())
    grid.append(list(input()))
    grid.append(list(input()))

    if bfs(grid, n):
        print("YES")
    else:
        print("NO")