"""
https://binarysearch.com/problems/Longest-Consecutive-Duplicate-String
"""

def solve(s):
    l = len(s)

    if l == 0:
        return 0

    i = 0
    j = 1
    count = 1
    max_count = count
    while i < l and j < l and i < j:
        if s[i] == s[j]:
            j += 1
            count += 1
        else: 
            i = j
            j = i + 1
            if count > max_count:
                max_count = count
            count = 1
    if count > max_count:
        max_count = count
    return max_count

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check(4, solve('abbbba'), 'abbbba')