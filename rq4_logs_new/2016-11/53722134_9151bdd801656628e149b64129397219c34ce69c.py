"""
Quick Sort
- Worst Time Complexity is O(n^2) when the list is already sorted.
- Average Time Complexity is O(nlogn).
- Space Complexity: O(n)
"""


def quick_sort(arr):
    start = 0
    end = len(arr)-1
    sort(arr, start, end)
    return arr


def sort(arr, start, end):
    if start < end:
        pivot = partition(arr, start, end)
        sort(arr, start, pivot-1)
        sort(arr, pivot+1, end)


def partition(arr, start, end):
    x = arr[end]
    i = start
    for j in range(start, end):
        if arr[j] <= x:
            arr[i], arr[j] = arr[j], arr[i]
            i += 1
    arr[i], arr[end] = arr[end], arr[i]
    return i

if __name__ == '__main__':
    test = [5, 13, 2, 25, 7, 17, 20, 8, 4]
    quick_sort(test)
    print(test)