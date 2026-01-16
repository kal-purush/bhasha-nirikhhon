"""
https://binarysearch.com/problems/Shortest-String
"""

def solve(s):
    m = {}
    for i in s:
        if i in m:
            m[i] += 1
        else:
            m[i] = 1
    
    cnt_1 = 0
    cnt_0 = 0
    if '1' in m:
        cnt_1 = m['1']
    if '0' in m:
        cnt_0 = m['0']
    
    return abs(cnt_1 - cnt_0)

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check(1, solve('11000'), '11000')
            
    