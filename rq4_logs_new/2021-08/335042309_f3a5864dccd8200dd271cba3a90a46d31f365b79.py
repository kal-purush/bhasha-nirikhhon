def format_license_key(s, k):
    
    s_norm = s.replace('-','').upper()
    if k>=len(s_norm):
        return s_norm.upper()
    s_norm = s.replace('-','').upper()
    s_final = ''
    c = 0

    for i in range(len(s_norm)-1,-1,-1):
        if c==k:
            s_final = '-' + s_final
            c = 0
        s_final = s_norm[i] + s_final 
        c += 1

    return s_final 

test1 = "5F3Z-2e-9-w"
print(format_license_key(test1, 4))

test2 = "2-5g-3-J"
print(format_license_key(test2, 2))

test3 = ""
print(format_license_key(test3, 2))

test4 = "1-2a-3"
print(format_license_key(test4, 5))