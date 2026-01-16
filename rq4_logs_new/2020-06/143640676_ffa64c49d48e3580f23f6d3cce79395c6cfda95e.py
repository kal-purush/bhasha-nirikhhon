# problem statement -
# Find the indexes i and j so that [j-i] is maximum
# constraint arr[i] <= arr[j] and no sorting is done
arr = [20, 8, 10, 3, 2, 80, 30, 0, 10]
str = '5 14 62 55 27 60 45 3 3 7 85 22 43 69 29 90 73 9 59 99 37 9 54 49 4 34 34 49 91 55 68'
arr = []
for item in str.split(" "):
    arr.append(int(item))
print(len(arr))

# i should be always be on the left hand side of the array mid
# j should be always be on the right hand side of the array mid
def find_max_index_difference(arr):
    arr_len = len(arr)
    largest_difference = None
    for i in range(0, arr_len-1):
        for j in range(i + 1, arr_len):
            if largest_difference < (j - i) and arr[i] < arr[j]:
                largest_difference = (j - i)
                print(i,j)

    print(largest_difference)

def max_condition(arr):
    i=0
    j=len(arr)-1
    if arr[i] <= arr[j]:
        return j-i

find_max_index_difference(arr)