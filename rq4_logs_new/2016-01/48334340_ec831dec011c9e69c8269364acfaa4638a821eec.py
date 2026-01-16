def msort(arr,first,last):
    mid=first+(last-first)/2
    '''Divide the list into two equal parts and recursively sorting them.'''
    if first<last:
        msort(arr,first,mid)
        msort(arr,mid+1,last)

    a,f,l=0,first,mid+1
    temp=[None]*(last-first+1)
	'''Merging the lists into a temporary list in sorted manner.'''
    while f<=mid and l<=last:
        if arr[f]<=arr[l]:
            temp[a]=arr[f]
            f+=1
        else:
            temp[a]=arr[l]
            l+=1
        a+=1
    if f<=mid:
        temp[a:]=arr[f:mid+1]
    if l<=last:
        temp[a:]=arr[l:last+1]
    '''Adding the sorted list in original list.'''
    a=0
    while first<=last:
        arr[first]=temp[a]
        first+=1
        a+=1
'''A helper merge-sort function'''
def mergesort(arr):
    msort(arr,0,len(arr)-1)
    return arr
    
print mergesort([1,5,9,6,8,4,5])