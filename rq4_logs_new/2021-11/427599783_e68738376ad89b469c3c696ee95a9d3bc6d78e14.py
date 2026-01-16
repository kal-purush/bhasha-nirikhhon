"""
https://binarysearch.com/problems/Remove-One-Letter
"""

def solve(s0, s1):
    if len(s0) != len(s1) + 1:
        return False 
    
    for i in range(len(s1)):
        if s0[i] != s1[i]:
            return s1[i:] == s0[i+1:]

    return True

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check(False, solve('hello', 'hello'), ('hello', 'hello'))
res_check(True, solve('hello', 'hell'), ('hello', 'hell'))