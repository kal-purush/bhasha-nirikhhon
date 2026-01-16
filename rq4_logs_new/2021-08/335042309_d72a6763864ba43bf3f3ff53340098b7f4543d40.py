def find_strobogrammatic(n):

    flip_digs = {'0':'0', '1':'1', '6':'9', '8':'8', '9':'6'}

    if n==1:
        return [0, 1, 8]

    results = []

    for d in flip_digs.keys():
        strob_recur(results, d, n, 1, flip_digs)

    return results

def strob_recur(results, num, n, size, flip_digs):

    if size==n:
        results.append(num)
        return
    mid = 0
    if n%2==0:
        mid = n//2
    else:
        mid = n//2 + 1
        if size>=mid and num[n//2] in ['6','9']: # Can't be flipped
            return
    if size>=mid:
        next_cand = ''
        next_cand = num + flip_digs[num[n-size-1]]
        
        strob_recur(results, next_cand, n, size+1, flip_digs)
    else:
        next_cand = ''
        for d in flip_digs.keys():
            next_cand = num + d 
            strob_recur(results, next_cand, n, size+1, flip_digs)
            next_cand = ''

print(find_strobogrammatic(5))
print(find_strobogrammatic(4))
print(find_strobogrammatic(3))      
print(find_strobogrammatic(2))   