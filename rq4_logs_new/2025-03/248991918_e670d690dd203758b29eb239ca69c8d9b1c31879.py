class Solution:
  def solveCoinSum(self, nums, target):
    cache = {}
    def dfs(i, amount):
      if amount == target:
        return 1
      
      if amount > target:
        return 0

      if i == len(nums):
        return 0
      
      coordinate = (i, amount)

      if coordinate in cache:
        return cache[coordinate]

      current_num = nums[i]
      # cache[coordinate] = dfs(i, amount + current_num) + dfs(i + 1, amount)

      # return cache[coordinate]
      return dfs(i, amount + current_num) + dfs(i + 1, amount)
    
    return dfs(0, 0)



s = Solution()
print(s.solveCoinSum([1, 2, 5], 5))