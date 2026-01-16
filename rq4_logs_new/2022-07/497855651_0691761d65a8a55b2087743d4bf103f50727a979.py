# ######################## merge sort code #################################

def merge_sort(arr):
    if len(arr) == 0:
        return 'EMPTY ARRAY'
    elif len(arr) == 1:
        return arr
    else:
        mid = len(arr) // 2
        left = arr[:mid]
        right = arr[mid:]

        # Recursive call on the left side
        merge_sort(left)
        # Recursive call on the right side
        merge_sort(right)

        i = 0
        j = 0
        k = 0

        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                arr[k] = left[i]
                i += 1
            else:
                arr[k] = right[j]
                j += 1
            k += 1

        # set remaining entries in arr to remaining values in left
        while i < len(left):
            arr[k] = left[i]
            i += 1
            k += 1

        # set remaining entries in arr to remaining values in right
        while j < len(right):
            arr[k] = right[j]
            j += 1
            k += 1
        # return the sorted array
        return arr


# ######################## merge sort code #################################


if __name__ == "__main__":
    print(merge_sort([]))  # EMPTY ARRAY
    print(merge_sort([1]))  # [1]
    print(merge_sort([8, 4, 23, 42, 16, 15]))  # [4, 8, 15, 16, 23, 42]
    print(merge_sort([20, 18, 12, 8, 5, -2]))  # [-2, 5, 8, 12, 18, 20]
    print(merge_sort([5, 12, 7, 5, 5, 7]))  # [5, 5, 5, 7, 7, 12]
    print(merge_sort([2, 3, 5, 7, 13, 11]))  # [2, 3, 5, 7, 11, 13]