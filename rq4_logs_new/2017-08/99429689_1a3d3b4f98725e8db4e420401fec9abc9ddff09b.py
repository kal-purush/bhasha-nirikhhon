

def karatsuba(num,n,a,b,c,d,i = 0):
    i += 1
    if(i == 1):
        sum =  (10** n) *num   +   karatsuba(a * d + b * c,n// 2,a,b,c,d,i)
        return sum
    elif(i == 2):
        sum =  (10** n) *num   +   karatsuba(None,None,a,b,c,d,i)
        return sum
    elif(i == 3):
        return (b * d)




num1 =  input().strip()
num2  =  input().strip()
n = len(num1)
num1 = int(num1)
num2 = int(num2)
a  =  num1//(10** (n// 2))
b  =  num1%(10** (n// 2))
c =   num2//(10** (n// 2))
d =   num2%(10** (n// 2))
product  =  karatsuba(a * c,n,a,b,c,d)
print("The product:",product)





