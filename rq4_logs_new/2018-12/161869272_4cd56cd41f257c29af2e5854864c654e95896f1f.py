def findMedianSortedArrays(self, nums1, nums2):
        """
        :type nums1: List[int]
        :type nums2: List[int]
        :rtype: float
        """
        
        total_nums = sorted(nums1 + nums2)
        
        len_nums = len(total_nums)
        median = 0
        if len_nums % 2 != 0:
            median = total_nums[(len_nums-1)//2] 
        else:
            median = (total_nums[(len_nums-1)//2] + total_nums[(len_nums-1)//2 + 1]) / 2
        return median