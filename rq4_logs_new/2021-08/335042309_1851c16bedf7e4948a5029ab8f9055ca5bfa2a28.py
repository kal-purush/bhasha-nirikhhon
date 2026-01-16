def longest_palindrome(s):

    n = len(s)

    if n==0:
        return ''
    elif n==1:
        return s 

    start = -1 
    end = -1 

    for i in range(n):
        l1 = expand_centre(s, i, i)
        l2 = expand_centre(s, i, i+1)

        l = max(l1, l2)

        if l>end-start:
            start = i - (l-1)//2
            end = i + l//2
    
    return s[start:end+1]

def expand_centre(s, i, j):

    n = len(s)
    
    st = i
    ed = j

    while st>=0 and ed<n and s[st] == s[ed]:
            st -= 1
            ed += 1
    
    return ed - st - 1

s = "babad"
longest_palindrome(s)

s = "cbbd"
longest_palindrome(s)

s = "a"
longest_palindrome(s)

s = "ac"
longest_palindrome(s)    

s = "aaaaaa"
longest_palindrome(s) 

s = "abababa"
longest_palindrome(s) 

s = "abbaccddcca"
longest_palindrome(s) 


    

    