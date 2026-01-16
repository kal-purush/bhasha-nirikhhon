class Solution:
    def findDifference(self, nums1: List[int], nums2: List[int]) -> List[List[int]]:
        nums1_missing = []
        nums2_missing = []

        for num in nums1:
            if num not in nums2 and num not in nums2_missing:
                nums2_missing.append(num)

        for num in nums2:
            if num not in nums1 and num not in nums1_missing:
                nums1_missing.append(num)

        return [nums2_missing, nums1_missing]