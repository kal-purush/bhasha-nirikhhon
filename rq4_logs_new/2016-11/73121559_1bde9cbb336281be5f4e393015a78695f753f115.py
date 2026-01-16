def palindrome(number):
    last=len(number)-1
    for i in range(0,len(number)/2):
        if number[i]!=number[last]:
            return False
        last-=1
    return True
    
def mpalindrome(number):

    while not palindrome(number):
        reverse=""
        for digit in range((len(number)-1),-1,-1):
            reverse+=number[digit]
                
        nnumber=int(number)+int(reverse)
        number=str(nnumber)
        
    
    return number

#main
#print palindrome("114411")
#print palindrome("11444")
#print palindrome("1111")
print mpalindrome("1111")
print mpalindrome("122")
print mpalindrome("1223")
print mpalindrome("4556")