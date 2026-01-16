"""
https://binarysearch.com/problems/Number-of-Unique-Character-Substrings
"""
def solve(s):
    def get_all_substr_len(s):
        l = len(s)
        if l == 0:
            return []
        substr_counts = []
        i = 0
        j = 1
        count = 1
        cur_char = s[i]
        while i < l  and j < l:
            if cur_char == s[j]:
                j += 1
                count += 1
            else:
                i = j
                cur_char = s[i]
                j = i + 1
                substr_counts.append(count)
                count = 1
        substr_counts.append(count)
        return substr_counts
    res = get_all_substr_len(s)
    return sum([(i * (i+1)) // 2 for i in res])

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check(4, solve('aab'), 'aab')