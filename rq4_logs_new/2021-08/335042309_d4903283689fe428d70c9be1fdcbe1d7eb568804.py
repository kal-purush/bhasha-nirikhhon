def is_isomorphic(s, t):

    n = len(s)

    if n==0:
        return True 

    sdict = {}
    tdict = {}

    k = 0
    l = 0
    for i in range(n):
        if s[i] not in sdict:
            k += 1
            sdict[s[i]] = k
        if t[i] not in tdict:
            l += 1
            tdict[t[i]] = l 

        
    for i in range(n):
        if sdict[s[i]]!=tdict[t[i]]:
            return False
    
    return True 

is_isomorphic("egg", "add")
is_isomorphic("foo", "bar")
is_isomorphic("paper", "title")
is_isomorphic("paper", "abcde")