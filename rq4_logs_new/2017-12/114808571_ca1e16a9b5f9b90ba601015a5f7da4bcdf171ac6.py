class Solution:
    def findMedianSortedArrays(self, nums1, nums2):
        """
        :type nums1: List[int]
        :type nums2: List[int]
        :rtype: float
        """
        len1 = len(nums1)
        len2 = len(nums2)
        if len2 > len1:
            nums1, len1, nums2, len2 = nums2, len2, nums1, len1

        left = 0
        right = 2 * len2

        while left <= right:
            j = (left + right) >> 1
            i = len1 + len2 - j
            l1 = -float("inf") if i == 0 else nums1[(i - 1) >> 1]
            l2 = -float("inf") if j == 0 else nums2[(j - 1) >> 1]

            r1 = float("inf") if i == len1 * 2 else nums1[i >> 1]
            r2 = float("inf") if j == len2 * 2 else nums2[j >> 1]

            if l1 > r2:
                left = j + 1
            elif l2 > r1:
                right = j - 1
            else:
                return (max(l1, l2) + min(r1, r2)) / 2.0


if __name__ == '__main__':
    s = Solution()
    print(s.findMedianSortedArrays([1,2,3,4,5],[3,6,9,10,59,90]))