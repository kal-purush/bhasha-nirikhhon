# Insertion sort implementation using Python

def insertionSort(arr):
    n = len(arr)

    # Traverse through 1 to length of array
    for i in range(1, n):
        key = arr[i]

        # Move elements of array, that are greater
        # than key, to one position ahead of their
        # current position
        j = i - 1
        while j >= 0 and key < arr[j]:
            arr[j+1] = arr[j]
            j -= 1
        arr[j+1] = key

    return arr