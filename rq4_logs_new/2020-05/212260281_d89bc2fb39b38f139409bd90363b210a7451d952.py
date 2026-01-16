# DESCRIPTION
# Given a string containing only three types of characters: '(', ')' and '*',
# write a function to check whether this string is valid.
# We define the validity of a string by these rules:
    # Any left parenthesis '(' must have a corresponding right parenthesis ')'.
    # Any right parenthesis ')' must have a corresponding left parenthesis '('.
    # Left parenthesis '(' must go before the corresponding right parenthesis ')'.
    # '*' could be treated as a single right parenthesis ')'
    # or a single left parenthesis '(' or an empty string.
    # An empty string is also valid.

# EXAMPLE 1:
# Input: "()"
# Output: True

# EXAMPLE 2:
# Input: "(*))"
# Output: True

class Solution:
    def checkValidString(self, s: str) -> bool:
        '''
        Time: O(N), where N is the length of the string
        Space: O(1), constant space no aux space used
        '''
        # Greedy Algorithm
        # increments at '(' dec for ')'
        cmin = 0
        # incs '(' and '*' decs for ')'
        cmax = 0

        for i in s:
            if i == '(':
                cmax += 1
                cmin += 1
            if i == ')':
                cmax -= 1
                # not including itself find the max between cmin-1 and 0
                # this makes sure cmin is not negative
                cmin = max(cmin - 1, 0)
            if i == '*':
                cmax += 1
                cmin = max(cmin - 1, 0)
            if cmax < 0:
                return False

        return cmin == 0