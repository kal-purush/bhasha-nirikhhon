def longest_substring(s):
    n = len(s)
    if n<=2:
        return n

    single = [0]*n
    double = [0]*n

    single_char = s[1] # Keeps track of longest single character substring
    double_char = [s[0], s[1]] # Keeps track of longest double character substring

    single[0] = 1 
    single[1] = 2 if s[0]==s[1] else 1
    double[0] = 1
    double[1] = 2
    
    for i in range(2,n):
        if s[i] in double_char:
            if s[i]==single_char:
                single[i] = single[i-1] + 1
            else:
                single[i] = 1
                single_char = s[i]
            double[i] = max(single[i-1], double[i-1]) + 1
        else:
            double_char = [s[i-1],s[i]]
            double[i] = max(2, 1+single[i-1])
            single_char = s[i]
            single[i] = 1
        
    return max(double)




        


        