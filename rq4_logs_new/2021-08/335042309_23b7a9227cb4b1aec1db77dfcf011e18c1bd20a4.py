def find_median_sorted_arrays(nums1, nums2):

    if len(nums1)>len(nums2):
        nums1, nums2 = nums2, nums1

    n1 = len(nums1)
    n2 = len(nums2)

    l = 0
    r = n1

    truemid = (n1+n2+1)//2

    while l<=r:
        mid = (l+r)//2
        l1 = mid # Left side of merged array in nums1
        l2 = truemid - mid # Left side of merged array in nums2

        if l1==0:
            left1 = -float('inf')
        else:
            left1 = nums1[mid-1]
        
        if l1==n1:
            right1 = float('inf')
        else:
            right1 = nums1[mid]

        if l2==0:
            left2 = -float('inf')
        else:
            left2 = nums2[l2-1]
        
        if l2==n2:
            right2 = float('inf')
        else:
            right2 = nums2[l2]

        if left1<=right2 and left2<=right1: # We've come to the median
            if (n1+n2)%2==0:
                return (max(left1, left2) + min(right1, right2))/2.0
            else:
                return max(left1, left2)

        if left1>right2:
            r = mid-1
        else:
            l = mid+1
    
    return 

nums1 = [1,3]
nums2 = [2]
find_median_sorted_arrays(nums1, nums2)

nums1 = [1,2]
nums2 = [3,4]
find_median_sorted_arrays(nums1, nums2)

nums1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
nums2 = [11, 12, 13, 14, 15, 16, 17, 18, 19]
find_median_sorted_arrays(nums1, nums2)

nums1 = [2, 3, 5, 8]
nums2 = [10, 12, 14, 16, 18, 20]
find_median_sorted_arrays(nums1, nums2)       
            


