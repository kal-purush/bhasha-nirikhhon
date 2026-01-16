# Time - O(N)

# Space - O(N)

Algo - (Solving by stroing respective indicies)
We use a stack to go through the elements in the given nums
We calculate the stack top element and add the respective element to our output array
We keep on adding elements and pop them once they find their respective greater element
(While to cover the greater elements at farther distance)
class Solution:
    def dailyTemperatures(self, T: List[int]) -> List[int]:
        if not T or len(T)>30000:
            return []
        
        stack = list()
        
        ans = [0 for i in range(len(T))]
        
        for i in range(len(T)):            
            
            while stack and  T[i] > T[stack[-1]]:
                temp = stack.pop()
                ans[temp] = i - temp    
            
            stack.append(i)           
                    
        return ans     