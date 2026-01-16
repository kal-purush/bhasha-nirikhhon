n = 4
t = ['w'] * 5
t = t + [2]
print(t)

candidates = [2,5,2,1,2]
used = [0 for i in range(len(candidates))]
target = 5
res = []
candidates.sort()

def backTracking(nowInd, path):
    if sum(path) == target:
        res.append(path[:])
        return
    if nowInd == len(candidates):
        return
    
    for i in range(nowInd, len(candidates)):
        if i > 0 and candidates[i] == candidates[i - 1] and used[i - 1] == 0:
            continue
        path.append(candidates[i])
        used[i] = 1
        if sum(path) > target:
            path.pop()
            return
        backTracking(i + 1, path)
        path.pop()
        used[i] = 0

backTracking(0, [])
print(res)


# nums = [0, 1, 2, 3]
# nums = [0]
nums = [1, 2, 3]
res = []

def backTracking(startIndex, path):
    res.append(path[:])
    for i in range(startIndex, len(nums)):
        path.append(nums[i])
        backTracking(i + 1, path)
        path.pop()


backTracking(0, [])
print(res)



nums = [1, 2, 3]
res = [[]]
for i in nums:
    res = res + [x + [i] for x in res]    
print(res)




nums = [1,2,2]
nums.sort()
res = []
def DFS(nowInd, path):
    res.append(path[:])
    
    for i in range(nowInd, len(nums)):
        if i > nowInd and nums[i] == nums[i - 1]:
            continue
        DFS(i + 1, path + [nums[i]])
    
DFS(0, [])
print(res) 


nums = [1,2,3,4,5,6,7,8,9,10,1,1,1,1,1]
res = []
def DFS(nowInd, path):
    if len(path) > 1:
        res.append(path[:])
    
    for i in range(nowInd, len(nums)):
        if i > nowInd and nums[i] in nums[nowInd:i]:
            continue
        if len(path) == 0 or nums[i] >= path[-1]:
            DFS(i + 1, path + [nums[i]])

DFS(0, [])
print(len(res))

nums = [1,2,3]
res = []


def DFS(path):
    if len(path) == len(nums):
        res.append(path[:])
        return
    for i in range(len(nums)):
        if nums[i] in path:
            continue
        DFS(path + [nums[i]])

DFS([])
print(res)




nums = [1,1,2,3]
res = []
nums.sort()
used = [0 for i in range(len(nums))]
def DFS(path):
    if len(path) == len(nums):
        res.append(path[:])
        return 
    
    for i in range(len(nums)):
        if used[i] == 1 or (i > 0 and nums[i - 1] == nums[i] and used[i - 1] == 0):
            continue
        used[i] = 1
        DFS(path + [nums[i]])
        used[i] = 0
DFS([])            

n = 4
res = []

def isValid(depth, y, path):
    for i in range(len(path)):
        if path[i] == y or abs(depth - i) == abs(y - path[i]):
            return False
    return True

def DFS(depth, path):
    if len(path) == n:
        temp = [['.'] * n for i in range(n)]
        for i in range(n):
            temp[i][path[i]] = 'Q'
        res.append([''.join(t) for t in temp])
        return

    for j in range(n):
        if isValid(depth, j, path):
            path.append(j)
            DFS(depth + 1, path)
            path.pop()

DFS(0, [])
print(res)



n = 4
res = []
def isValid(x, y, path):
    if path == []:
        return True
    for i in path:
        if i[1] == y:
            return False
    for i in path:
        if abs(i[0] - x) == abs(i[1] - y):
            return False
    return True


def DFS(depth, path):
    if len(path) == n:
        temp = [['.'] * n for _ in range(n)]
        for i in path:
            temp[i[0]][i[1]] = 'Q'
        res.append([''.join(t) for t in temp])
        # temp.append([''.join(r) for r in cur])
        return

    for i in range(depth, n):
        for j in range(n):
            if isValid(i, j, path):
                path.append([i, j])
                DFS(i + 1, path)
                path.pop()

DFS(0, [])
print(res)




