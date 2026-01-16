arr = [['a', 2, 100], ['b', 1, 40], ['c', 2, 80], ['d', 1, 20], ['e', 3, 60]]
print("Following is maximum profit sequence of jobs")
n = len(arr)
t = 3
for i in range(n):
   for j in range(n - 1 - i):
     if arr[j][2] < arr[j + 1][2]:
       arr[j], arr[j + 1] = arr[j + 1], arr[j]
result = [False] * t
job = ['-1'] * t
for i in range(len(arr)):
   for j in range(min(t - 1, arr[i][1] - 1), -1, -1):
     if result[j] is False:
       result[j] = True
       job[j] = arr[i][0]
       break
print(job)