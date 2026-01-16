"""

"""
def solve(s, c):
        res = []
        prev = float('-inf')
        for i, item in enumerate(s):
            if c == item:
                prev = i
            if prev != -1:
                # atleast found once 
                res.append(i - prev)
            else:
                res.append(prev)
        
        l = len(s)
        prev = float('inf') #largest
        for i in range(l-1, -1, -1):
            if s[i] == c:
                prev = i
            res[i] = min(res[i], prev - i)
        return res

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

s = "aabaab"
c = "b"
res_check([2,1,0,1,1,0], solve(s, c), (s, c))

s = "aaaab"
c = "b"
res_check([4, 3, 2, 1, 0], solve(s, c), (s, c))