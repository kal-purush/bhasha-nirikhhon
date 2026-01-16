def binarySearch(arr, l, r, x):
    if r >= l:
        mid = l + (r - l) // 2
        if arr[mid] == x:
            return mid
        elif arr[mid] > x:
            return binarySearch(arr, l, mid - 1, x)
        else:
            return binarySearch(arr, mid + 1, r, x)
    else:
        return -1

arr=list(map(int,input("enter your list").split()))
x=int(input("Enter the number you want to search"))
result = binarySearch(arr, 0, len(arr) - 1, x)
if result != -1:
    print("Element is contained in the list at index %d" % result)
else:
    print("Element is not contained in the list")