"""
https://binarysearch.com/problems/Reverse-Words
"""

def solve(sentence):
    if len(sentence) <= 1:
            return sentence
    l = sentence.split(' ')
    n = len(l)
    i = 0
    j = len(l) - 1
    while i < n // 2 and j >= n // 2 :
        temp = l[i]
        l[i] = l[j]
        l[j] = temp
        i += 1
        j -= 1
    return ' '.join(l)

# using list reverse method
def solve_1(sentence):
    if len(sentence) <= 1:
            return sentence
    l = sentence.split(' ')
    l.reverse()
    return ' '.join(l)

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

s1 = "hello there my friend"
res = "friend my there hello"
res_check(res, solve(s1), s1)

res_check(res, solve_1(s1), s1)