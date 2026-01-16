class Solution:
    
    def isValid(self, s: str) -> bool:
        # Check if input (only brackets/parenteses) are opened and closed correctly
        stack = []
        relate = {'(':')', '{':'}', '[':']'}
        
        for char in s:
            if char in relate.keys():
                stack.append(char)
            else:
                if len(stack) == 0 or relate[stack.pop()] != char:
                    return False
        
        return True if len(stack) == 0 else False