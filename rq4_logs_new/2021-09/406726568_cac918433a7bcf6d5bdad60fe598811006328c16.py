#!/usr/bin/python

#Calculator script

number_1 = int(input('Enter your first number: '))

command = input('Enter you command: ')

number_2 = int(input('Enter your second number: '))

if command =='+':
    res_1 = number_1 + number_2 
    print('The result is: ', res_1)
elif command =='-':
    res_2 = number_1 - number_2 
    print('The result is: ', res_2)  
elif command == '*':
    res_3 = number_1 * number_2
    print('The result is: ', res_3)
elif command == '/':
    res_4 = number_1 / number_2 
    print('The result is: ',res_4)
elif command == '//':
    res_5 = number_1 // number_2 
    print('The result is: ', res_5) 
elif command == '**':
    res_6 = number_1 ** number_2 
    print('The result is: ',res_6)
else:
    if command == '%':
        res_7 = number_1 % number_2 
        print('The result is: ',res_7) 
