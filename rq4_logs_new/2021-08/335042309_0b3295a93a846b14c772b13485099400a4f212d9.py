def plus_one(digits):
    carry = 1

    for i in range(len(digits)-1,-1,-1):
        digits[i] += carry 
        if digits[i]>9:
            carry = digits[i]//10
            digits[i] %= 10
        else:
            carry = 0
        
        if carry==0:
            break
    
    if carry>0:
        return [carry] + digits 
    
    return digits

if __name__=="__main__":
    test1 = [1,2,3]
    print(plus_one(test1))

    test2 = [4,3,2,1]
    print(plus_one(test2))

    test3 = [0]
    print(plus_one(test3))

    test4 = [9]
    print(plus_one(test4))