def dfs(v):
    visited[v] = True
    for i in graph[v]:
        if not visited[i]:
            dfs(i)


n, m = map(int, input().split())
edges = [list(map(int, input().split())) for _ in range(m)]
if m == 0:
    print(n)
else:
    graph = [[] for _ in range(n + 1)]
    for edge in edges:
        graph[edge[0]].append(edge[1])
        graph[edge[1]].append(edge[0])
    cnt = 0
    visited = [False] * (n + 1)
    cntVillage = 0
    for i in range(1, n + 1):
        if len(graph[i]) == 0:
            cntVillage += 1
            visited[i] = True
    for i in range(1, n + 1):
        if not visited[i]:
            cnt += 1
            dfs(i)
    print(abs(cnt - cntVillage))