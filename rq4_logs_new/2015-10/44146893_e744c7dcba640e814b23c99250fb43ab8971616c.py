import math

def CountSplitInversion(left_array, right_array):
    left_len = len(left_array)
    right_len = len(right_array)
    array = []
    i = 0
    j = 0
    count = 0

    for k in range(left_len + right_len):
        if i == left_len:
            array += right_array[j:right_len]
            break
        if j == right_len:
            array += left_array[i:left_len]
            break

        if left_array[i] < right_array[j]:
            array.append(left_array[i])
            i += 1
        elif left_array[i] > right_array[j]:
            array.append(right_array[j])
            j += 1
            count += left_len - i
    #print array, count
    return array, count

def CountInversion(array):
    n = len(array)
    middle = int(math.floor(n/2))
    #print middle, n
    if(n==1):
        return array, 0

    left_array, left_count = CountInversion(array[0:middle])
    right_array, right_count = CountInversion(array[middle:n])
    final_array, split_count = CountSplitInversion(left_array, right_array)
    return final_array, left_count + right_count + split_count