def candy(ratings):
    n = len(ratings)

    if n==1:
        return 1

    min_candies = 0
    l2r = [1]*len(ratings)
    r2l = [1]*len(ratings)

    for i in range(1,n):
        if ratings[i-1]<ratings[i]:
            l2r[i] = l2r[i-1] + 1
    
    for i in range(n-2,-1,-1):
        if ratings[i+1]<ratings[i]:
            r2l[i] =r2l[i+1] + 1

    for i in range(n):
        min_candies += max(l2r[i],r2l[i])

    return min_candies

ratings = [1, 0, 2]
candy(ratings)

ratings = [1,2,2]
candy(ratings)

ratings = [2,2,2,2]
candy(ratings)

ratings = [1,2]
candy(ratings)

ratings = list(range(1,10))
candy(ratings)