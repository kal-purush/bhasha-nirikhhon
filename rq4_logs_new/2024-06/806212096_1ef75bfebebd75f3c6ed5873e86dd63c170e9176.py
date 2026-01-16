def bubble_sort(arr: list):
    array = arr.copy()    
    for i in range(len(array) - 1):
        swapped = False
        for j in range(len(array) - 1- i):
            if array[j] > array[j+1]:
                array[j], array[j+1] = array[j+1], array[j]
                swapped = True
        if not swapped:
            break
    return array
arry = [2, 5, 12, 6, 8, 7, 1, 9, 10, 3]
arr_2 = [7, 3, 9, 12, 11]

print("Array before sorting: ", arry)

new_array = bubble_sort(arry)
print("Array after sorting: ", new_array)