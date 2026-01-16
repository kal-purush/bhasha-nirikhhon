class Solution:
    def numberOfSubarrays(self, nums: List[int], k: int) -> int:
        prefix_sums = []
        count = 0
        last_odd_occurrence = -1

        for index, num in enumerate(nums):
            is_odd = num%2 != 0
            if is_odd:
                if len(prefix_sums) >= k:
                    count += prefix_sums[-k] * (index - last_odd_occurrence)
                prefix_sums.append(index-last_odd_occurrence)
                last_odd_occurrence = index

        index += 1
        if len(prefix_sums) >= k:
            count += prefix_sums[-k] * (index - last_odd_occurrence)
        
        return count