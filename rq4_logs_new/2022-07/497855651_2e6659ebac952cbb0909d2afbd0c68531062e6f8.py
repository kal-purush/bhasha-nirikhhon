# ######################## merge sort code #################################
def quick_sort(arr, left, right):
    if len(arr) == 0:
        return 'EMPTY ARRAY'
    if left < right:
        position = partition(arr, left, right)
        quick_sort(arr, left, position - 1)
        quick_sort(arr, position + 1, right)
    return arr


def partition(arr, left, right):
    pivot = arr[right]
    low = left - 1
    for i in range(left, right):
        if arr[i] <= pivot:
            low += 1
            swap(arr, i, low)

    swap(arr, right, low + 1)
    return low + 1


def swap(arr, i, low):
    temp = arr[i]
    arr[i] = arr[low]
    arr[low] = temp


# ######################## merge sort code #################################

if __name__ == "__main__":
    a = [8, 4, 23, 42, 16, 15]
    print(quick_sort(a, 0, len(a)-1))  # [4, 8, 15, 16, 23, 42]