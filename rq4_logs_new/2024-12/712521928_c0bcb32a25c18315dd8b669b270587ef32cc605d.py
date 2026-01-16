"""
46. Permutations

Given an array `nums` of distinct integers, return all the possible permutations.
You can return the answer in any order.
"""

"""
Example 1:
Input: nums = [1,2,3]
Output: [[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]

Example 2:
Input: nums = [0,1]
Output: [[0,1],[1,0]]

Example 3:
Input: nums = [1]
Output: [[1]]
"""

from itertools import permutations
class Solution:
    def permute(self, nums: list[int]) -> list[list[int]]:
        
        list_of_nums = [list(i) for i in permutations(nums)]
        
        return list_of_nums

class Solution:
    def permute(self, nums: list[int]) -> list[list[int]]:
        
        results = []
        used = [False] * len(nums)
        
        def backtracking(current_permutation):
            # Base Case
            if len(current_permutation) == len(nums):
                results.append(list(current_permutation))
                return

            #Backtracking
            for i in range(len(nums)):
                if not used[i]:
                    # 1. Choose nums[i]
                    current_permutation.append(nums[i])
                    used[i] = True
                    
                    # 2. Explore
                    backtracking(current_permutation)
                    
                    # 3. Backtrack
                    current_permutation.pop()
                    used[i] = False
    
        backtracking([])
        return results

sol = Solution()
print(sol.permute([1,3,2]))