class Solution:
    def isMonotonic(self, nums: List[int]) -> bool:
        stack1=[]
        stack=[]
        for n in nums:

            if stack and stack[-1]>n:
                stack.pop()
            stack.append(n)
            if stack1 and stack1[-1]<n:
                stack1.pop()
            stack1.append(n)
        print(stack)
        print(stack1)
        if len(stack)==len(nums) or len(stack1)==len(nums):
            return True
        return False
        


        

        