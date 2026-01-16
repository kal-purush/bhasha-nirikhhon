def bubble_sort(array):
    length = len(array)
    for i in range(length - 1):
        for j in range(length - 1):
            if array[j] > array[j + 1]:
                temp = array[j]
                array[j] = array[j + 1]
                array[j + 1] = temp
    return array


def selection_sort(arr):
    length = len(arr)
    min = arr[0]
    for i in range(length - 1):
        for j in range(length - 1):
            if arr[j + 1] < min:
                temp = arr[j]
                arr[j] = arr[j + 1]
                arr[j + 1] = temp
    return arr


def insertion_sort(arr):
    for i in range(1, len(arr)):
        for j in range(i - 1, -1, -1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]

    return arr


def merge_sorter(arr):
    if len(arr) == 1: return arr
    # split array into right and left
    length = len(arr)
    middle = (length) // 2
    left = arr[: middle]
    right = arr[middle:]

    return _merge_(
        merge_sorter(left),
        merge_sorter(right)
    )


def _merge_(left_ar, right_ar):
    result = []
    left_index =0
    right_index=0
    while left_index<len(left_ar) and right_index<len(right_ar):
        if left_ar[left_index]<=right_ar[right_index]:
            result.append(left_ar[left_index])
            left_index+=1
        else:
            result.append(right_ar[right_index])
            right_index+=1

    result += left_ar[left_index:] + right_ar[right_index:]
    return result


def _swap_(arr, first_index, second_index):
    temp = arr[first_index]
    arr[first_index] = arr[second_index]
    arr[second_index] = temp



def _partition_(arr, pivot, left, right):
    pivot_value = arr[pivot]
    partion_index = left

    for i in range(left, right):
        if arr[i]<pivot_value:
            _swap_(arr, i, partion_index)
            partion_index+=1
    _swap_(arr, right, partion_index)
    return partion_index


def quick_sorter(arr, left=0, right=None):
    length = len(arr)

    if right==None:
        right=length-1

    if left < right:
        pivot = right
        partion_index = _partition_(arr, pivot, left, right)

        quick_sorter(arr, left, partion_index - 1)
        quick_sorter(arr, partion_index + 1, right)
    return arr



numbers = [99, 44, 6, 2, 1, 5, 63, 87, 283, 4, 0]
numbers2 = [7,2,3,1,5,6,8]
numbers3 = [3,2,0,1]
print(quick_sorter(numbers))
