"""
https://binarysearch.com/problems/Run-Length-Encoding
"""

def solve(s):
    res = []
    l = len(s)
    if l > 0:
        i = 0
        cur = s[i]
        i = i + 1
        count = 1
        while i < l:
            while i < l and cur == s[i]:
                count += 1
                i = i + 1
            res.append(str(count) + cur)
            if i < l:
                cur = s[i]
                count = 0
    return ''.join(res)

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check('4a3b2c1d2a', solve("aaaabbbccdaa"), 'aaaabbbccdaa')
res_check('1a1b1c1d1e', solve("abcde"), 'abcde')