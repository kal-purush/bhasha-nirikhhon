
'''
You are climbing a stair case. It takes n steps to reach to the top.
Each time you can either climb 1 or 2 steps. In how many distinct ways can you climb to the top?
Note: Given n will be a positive integer.
'''


class Solution(object):
    def climbStairs(self, n):
        a = b = 1
        for _ in range(n):
            a, b = b, a + b
        return a


n = 10
s = Solution()
print(s.climbStairs(n))


'''
key intuition to solve the problem is that given a number of stairs N, 
if we know the number ways to get to the points [n-1] and [n-2] respectively,
denoted as n1 and n2 , then the total ways to get to the point [n] is n1 + n2. 
Because from the [n-1] point, we can take one single step to reach [n]. And from the [n-2] point, 
we could take two steps to get there. 
There is NO overlapping between these two solution sets, because we differ in the final step.
'''