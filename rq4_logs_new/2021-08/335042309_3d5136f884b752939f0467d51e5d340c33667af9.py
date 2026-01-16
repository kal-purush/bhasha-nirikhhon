def odd_jump_idx(arr):
    idx_map = {}
    n = len(arr)
    idx_map[n-1] = n-1

    for i in range(n-1):
        min_val = float('inf')
        min_ind = i
        for j in range(i+1,n):
            if arr[j]<arr[i]:
                continue 
            if arr[j]<min_val:
                min_val = arr[j]
                min_ind = j
        if min_ind == i: #No legal jumps present
            idx_map[i] = None
        idx_map[i] = min_ind

    return idx_map

def even_jump_idx(arr):
    idx_map = {}
    n = len(arr)
    idx_map[n-1] = n-1

    for i in range(n-1):
        max_val = -float('inf')
        max_ind = i
        for j in range(i+1,n):
            if arr[j]>arr[i]:
                continue 
            if arr[j]>max_val:
                max_val = arr[j]
                max_ind = j
        if max_ind == i: #No legal jumps present
            idx_map[i] = None
        idx_map[i] = max_ind

    return idx_map

def odd_even_jumps(arr):

    n = len(arr)
    if n==1:
        return 1

    odd_jumps = odd_jump_idx(arr)
    even_jumps = even_jump_idx(arr)

    c = 1 #For last index, always true

    for i in range(n-1):
        j = 1 #Jump counter
        start = i
        next = 0 #Check if this reaches last index
        while start<n:
            if j%2==0:
                next = even_jumps[start]
            else:
                next = odd_jumps[start]

            if next==n-1:
                c += 1
                break
            elif next==start: #Reached a dead end
                break 
            start = next
            j += 1

    return c
        
test1 = [10, 13, 12, 14, 15]
test2 = [2,3,1,1,4]

print(odd_even_jumps(test1))
print(odd_even_jumps(test2))