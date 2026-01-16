from numpy import random

arr = random.randint(1, 100, size=10)

arr = list(set(arr))
arr.sort()


def binary_search(array, target):
    left = 0
    right = len(array)

    while left <= right:
        mid = (left + right )//2

        if array[mid] == target:
            return mid

        if array[mid] < target:
            left = mid + 1
            
        else:
            right = mid - 1
            
    return -1

print("Array : ", arr)
ind = binary_search(arr, 4)
if ind != -1:
    print(f"Target 4 is found at index {ind}")
else:
    print("Target 4 is not found in array")