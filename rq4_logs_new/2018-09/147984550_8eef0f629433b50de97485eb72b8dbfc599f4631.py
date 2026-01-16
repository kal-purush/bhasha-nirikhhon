import math
import random

def main():
    numArr = [1, 4, 6, 2, 5, 3]
    quickSort(numArr, 0, len(numArr)-1)
    print(numArr)

def quickSort(numArr, front, end):
    if front < end:
        pivot = partition(numArr, front, end)
        quickSort(numArr, front, pivot-1)
        quickSort(numArr, pivot+1, end)

def partition(numArr, front, end):
    pivot = numArr[end]
    i = front - 1
    for j in range(front, end):
        if numArr[j] < pivot:
            i += 1
            swap(numArr, i, j)
    i+=1
    swap(numArr, i, end)
    return i

def swap(numArray, indexA, indexB):
    temp = numArray[indexA]
    numArray[indexA] = numArray[indexB]
    numArray[indexB] = temp

main()