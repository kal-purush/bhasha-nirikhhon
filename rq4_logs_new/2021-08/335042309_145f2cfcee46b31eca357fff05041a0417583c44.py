def merge(arr1, arr2):
    arr = []

    # if arr1==[]:
    #     return arr2
    # elif arr2==[]:
    #     return arr1
    i = 0
    j = 0

    while i<len(arr1) and j<len(arr2):
        if arr1[i]<=arr2[j]:
            arr.append(arr1[i])
            i += 1
        else:
            arr.append(arr2[j])
            j += 1
    
    while i<len(arr1):
        arr.append(arr1[i])
        i += 1

    while j<len(arr2):
        arr.append(arr2[j])
        j += 1

    return arr 

def merge_sort(arr):

    if len(arr)<=1:
        return arr

    mid = len(arr)//2

    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])

    return merge(left, right)

merge_sort([1,2,7,6,5])
