# MAKE A CALCULATOR
# num1 = float(input('enter a no'))
# num2 = float(input('enter a second number'))
# print('press 1 for addition\npress 2 for subtraction \npress 3 for multiplication \npress 4 for division')
# output = int(input('enter a value'))
# if output ==1:
#     print(num1+num2)
# elif output ==2:
#     print(num1 - num2)
# elif output ==3:
#     print(num1 * num2)
# elif output ==4:
#     print(num1 / num2)
import time
def add(num1,num2):
    return (num1+num2)
def subtract(num1,num2):
    return (num1-num2)
def multiply(num1,num2):
    return (num1*num2)
def division(num1,num2):
    return num1/num2

    print('pls select operation:\n 1)addition\n 2)substract\n 3)multipy \n4)division')
a = int(input('select operation from 1,2,3,4:'))
b = float(input('first no'))
c = float(input('enter a second no'))
if a == 1:
    print(b,'+',c,'=',add(b,c))
    time.sleep(5)
elif a == 2:
    print(b, '-', c, '=', subtract(b, c))
    time.sleep(5)
elif a==3:
    print(b, '*', c, '=', multiply(b, c))
    time.sleep(5)
elif a==4:
    try:
        print(b, '/', c, '=', division(b, c))
    except Exception as msg:
        print('error',msg)
        time.sleep(5)
else:
    print('invalid')
    time.sleep(5)