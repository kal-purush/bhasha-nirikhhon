"""
https://binarysearch.com/problems/Remove-Last-Duplicate-Entries
"""

def solve(nums):
    m = {}
    for i in nums:
        if i in m:
            m[i] += 1
        else:
            m[i] = 1

    nums1 = nums[:]
    nums1.reverse()
    for i in nums:
        if i in m and m[i] > 1:
            nums1.remove(i)
            m.pop(i)
    nums1.reverse()
    return nums1

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %d , got %d for input %s' % (expect, actual, input))

ip = [1, 3, 4, 1, 3, 5]
res = solve([1, 3, 4, 1, 3, 5])
res_check([1, 3, 4, 5], res, ip)

ip = [1, 1, 1, 2, 2, 2, 3, 3, 3]
res = solve(ip)
res_check([1, 1, 2, 2, 3, 3], res, ip)

ip = [6, 5, 1, 5, 1, 6]
res = solve(ip)
res_check([6, 5, 1], res, ip)