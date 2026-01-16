#피보나치수열 알리고리즘을 재귀함수와 for loop으로 비교하자
import time

def recurfibo(n):
    if n==0:
        return 0
    elif n==1:
        return 1
    else:
        return recurfibo(n-1) + recurfibo(n-2)

def iterfibo(n):
    fibo_list = []
    for i in range(0,n+1):
        if (i == 0) or (i == 1):
            fibo_list.append(i)
            point = i
        else:
            fibo_list.append(int(fibo_list[i-1]+fibo_list[i-2]))
            point = i

    return fibo_list[point]

while (True):
    num = int(input("Enter number: "))
    if num != -1:
        ts = time.time()
        answer = recurfibo(num)
        ts=time.time() - ts
        print("Recursive fibo answer =", answer, "The time required =", ts)
        print()
        ts = time.time()
        answer = iterfibo(num)
        ts=time.time() - ts
        print("iterative fibo answer =", answer, "The time required =", ts)
        print()