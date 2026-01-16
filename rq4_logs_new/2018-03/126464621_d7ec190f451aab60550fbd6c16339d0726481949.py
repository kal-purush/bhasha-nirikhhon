
num1 = 999
max_palindrome = 0
while num1 > 0:
    num2 = 999
    while num2 > 0:

        product = num1 * num2
        i = 0
        end = -1
        palindrome = True
        while i < len(str(product))/2:
            if str(product)[i] != str(product)[end]:
                palindrome = False
                break
            i+=1
            end-=1

        if palindrome:
            print(product)
            if product > max_palindrome:
                max_palindrome = product

        num2 -= 1
    num1 -= 1

print(max_palindrome)