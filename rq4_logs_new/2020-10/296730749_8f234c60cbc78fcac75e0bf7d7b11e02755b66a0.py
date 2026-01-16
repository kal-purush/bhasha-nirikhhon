from functools import reduce
# Given an unsorted array [1, 2, ..., n] which has a single element missing, determine the missing element in O(n) time and O(1) space.
def oddManOut(arr):
    return sum(range(max(arr)+1)) - sum(arr)

print(oddManOut([1,2,3,4,6,7,8,9]))

# Given an unsorted array [1, 1, 2, 2, 3, 3, ..., k, k] where every element is duplicated except one, find the missing element in O(n) time and O(1) space.
def missingDuplicate(arr):
    return reduce(lambda x, y: x ^ y, arr)

print(missingDuplicate([1,1,2,2,3,3,4,4,5,5,6,7,7,9,9]))

# Redo part 1 with two unknown elements
def missingTwo(arr, n):
    sum_missing = 0
    for i in range(0, n-2):
        sum_missing += arr[i]
    missing_sum = (n * (n + 1)) / 2 - sum_missing
    return missing_sum

print(missingTwo([1,2,3,5,6,7,9,10], 10))
# Redo part 2 with two unknown elements
