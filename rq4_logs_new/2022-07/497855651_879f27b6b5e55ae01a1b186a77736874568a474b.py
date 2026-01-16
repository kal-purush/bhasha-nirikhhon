# ######################## insertion sort code #################################

def insertion_sort(arr):
    """
    this function will take an array of integers and return it in sorted order
    """
    if len(arr) == 0:
        return 'EMPTY ARRAY'
    elif len(arr) == 1:
        return arr
    else:
        for i in range(1, len(arr)):
            j = i - 1
            temp = arr[i]

            while j >= 0 and temp < arr[j]:
                arr[j + 1] = arr[j]
                j = j - 1

            arr[j + 1] = temp

        return arr


# ######################## insertion sort code #################################
if __name__ == "__main__":
    print(insertion_sort([]))  # EMPTY ARRAY
    print(insertion_sort([1]))  # [1]
    print(insertion_sort([8, 4, 23, 42, 16, 15]))  # [4, 8, 15, 16, 23, 42]
    print(insertion_sort([20, 18, 12, 8, 5, -2]))  # [-2, 5, 8, 12, 18, 20]
    print(insertion_sort([5, 12, 7, 5, 5, 7]))  # [5, 5, 5, 7, 7, 12]
    print(insertion_sort([2, 3, 5, 7, 13, 11]))  # [2, 3, 5, 7, 11, 13]