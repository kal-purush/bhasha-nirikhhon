def fruit_baskets(fruits):

    if len(fruits)==1:
        return 1
    elif len(fruits)==2:
        return 2 

    double = [0]*len(fruits) #Longest sequence of 2 numbers
    single = [0]*len(fruits) #Longest sequence of a single number

    double[0] = 1 
    double[1] = 2
    single[0] = 1 
    if fruits[0]==fruits[1]:
        single[1] = 2
        double_basket = [fruits[0]]
    else:
        double_basket = [fruits[0], fruits[1]]
        single[1] = 1

    single_basket = fruits[1] #Basket for 1-sequence

    for i in range(2,len(fruits)):

        if fruits[i] in double_basket:
            if fruits[i] == single_basket:
                double[i] = max(1+double[i-1], 1 + single[i-1])
                single[i] = 1 + single[i-1]
            else: #Single basket needs to change
                single[i] = 1
                single_basket = fruits[i]
                double[i] = 1+double[i-1] #This means we are at an alternating part
        else: #Baskets need to change
            if len(double_basket)==1: #We're still at the first basket value
                double_basket.append(fruits[i])
                double[i] = 1+double[i-1]
            else:
                double_basket = [fruits[i-1],fruits[i]]
                double[i] = 1 + single[i-1]
            single_basket = fruits[i]
            single[i] = 1

    return max(double)

test1 = [1,1,1,1,1]
fruit_baskets(test1)

test2 = [1,2,1]
fruit_baskets(test2)

test3 = [0,1,2,2]
fruit_baskets(test3)

test4 = [0,1,2,3,4,7,8,7,8,7,8]
fruit_baskets(test4)

test5 = [0,0,1,1]
fruit_baskets(test5)

test6 = [0,1,0,1]
fruit_baskets(test6)

test7 = [1,2,3,2,1]
fruit_baskets(test7)

test8 = [0,1,6,6,4,4,6]
fruit_baskets(test8)