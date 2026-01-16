import sys
sys.stdin=open("1244_input.txt")
N=int(input())
switches=list(map(int,input().split()))
num_students = int(input())
students = []
for _ in range(num_students):
    gender, num = map(int, input().split())
    students.append((gender, num))

for gender, num in students:
    if gender == 1: 
        for i in range(num-1, N, num): 
            switches[i] = 1 - switches[i]
    elif gender == 2:  
        num -= 1 
        switches[num] = 1 - switches[num]  
        
        left, right = num - 1, num + 1
        while left >= 0 and right < N and switches[left] == switches[right]:
            switches[left] = 1 - switches[left]
            switches[right] = 1 - switches[right]
            left -= 1
            right += 1
if N>20:
    for i in range(N):
        print(switches[i],end=' ')
        if (i+1)%20==0 and i+1!=N:
            print()
else:
    for i in switches:print(i,end=' ')