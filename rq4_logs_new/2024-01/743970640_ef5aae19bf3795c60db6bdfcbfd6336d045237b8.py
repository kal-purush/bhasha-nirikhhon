for i in range(312614, 312652):
    k=[]
    for j in range(1,i+1):
        if i%j==0:
            k.append(j)
    if len(k)==6:
        print(k)