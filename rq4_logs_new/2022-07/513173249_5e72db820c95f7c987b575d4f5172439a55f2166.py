"""
Построение кучи in place из входного массива
https://stepik.org/lesson/41235/step/6?unit=19819
10:30
"""
n = int(input()) # Число узлов в куче
nodes = list(map(int, input().split())) # Узлы

def parent_id(i):
    return (i-1)//2 if i != 0 else 0
def leftChild(i):
    return i*2 + 1
def rightChild(i):
    return i*2 + 2

counter, answer = 0, []
def buildHeap(array, n):
    def _shiftDown(array, i, size):
        global answer
        global counter
        min_ind = i
        
        left_child = leftChild(i)
        if left_child < size and array[left_child] < array[min_ind]:
            min_ind = left_child
            
        right_child = rightChild(i)
        if right_child < size and array[right_child] < array[min_ind]:
            min_ind = right_child
        if min_ind != i:
            counter+=1
            array[min_ind], array[i] = array[i], array[min_ind]
            answer += [[i, min_ind]]
            _shiftDown(array, min_ind, size)
    
    for i in range(n//2, -1, -1):
        _shiftDown(array, i, n)
             
buildHeap(nodes, n) 
print(counter)
for val in answer:
    print(' '.join(map(str, val)), end = '\n')
# -*- coding: utf-8 -*- 
'''
По данным n процессорам и m задач определите, для каждой из задач,
каким процессором она будет обработана.

Вход. Число процессоров n и последовательность чисел
t0, . . . , tm−1, где ti — время, необходимое на обработку i-й
задачи.

Выход. Для каждой задачи определите, какой процессор
и в какое время начнёт её обрабатывать, предполагая, что
каждая задача поступает на обработку первому освободившемуся процессору
'''

n, m = map(int, input().split()) # Число процессоров и задач
tasks = list(map(int, input().split()))

heap = [[0,i] for i in range(n)]

def shiftDown(i):
    minIndex = i
    if i*2+1 < n and heap[i*2+1] < heap[minIndex]:
        minIndex = i*2+1
    if i*2+2 < n and heap[i*2+2] < heap[minIndex]:
        minIndex = i*2+2
    if minIndex != i:
        heap[minIndex], heap[i] = heap[i], heap[minIndex]
        shiftDown(minIndex)

for task in tasks:
    print(heap[0][1], heap[0][0])
    heap[0][0] += task
    shiftDown(0)
    
# Решение с использованием встроенной библиотеки heapq
from heapq import heappush, heappop

n, m = map(int, input().split()) # Число процессоров и задач
tasks = list(map(int, input().split()))
heap = [(0,i) for i in range(n)]

for task in tasks:
    start, cpu = heappop(heap)
    print(cpu, start)
    heappush(heap, (start+task, cpu))
    
'''
Sample Input:

2 5
1 2 3 4 5
Sample Output:

0 0
1 0
0 1
1 2
0 4
'''