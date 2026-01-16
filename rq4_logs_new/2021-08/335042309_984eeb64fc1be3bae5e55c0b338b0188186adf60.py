def min_window_substring(s, t):

    m = len(s)
    n = len(t)
    if n>m:
        return ""

    l = 0
    r = 0

    t_dict = dict()

    for i in range(n):
        if t[i] in t_dict.keys():
            t_dict[t[i]] += 1
        else:
            t_dict[t[i]] = 1

    reqd = len(t_dict)
    formed = 0 

    window_counts = {}
    res = float('inf'), None, None

    while r<m:
        c = s[r]

        window_counts[c] = window_counts.get(c,0) + 1

        if c in t_dict and window_counts[c]==t_dict[c]:
                formed += 1 

        while l <= r and formed==reqd: 
            c = s[l]

            if r-l+1<res[0]:
                res = (r-l+1, l, r)
            
            window_counts[c] -= 1
            if c in t_dict and window_counts[c]<t_dict[c]:
                formed -= 1

            l += 1

        r += 1

    if res[0]==float('inf'):
        return ""

    return s[res[1]:res[2]+1]


        
        
