exp = input("Enter the expression (with space) : ")
exp = exp.split(" ")
if(exp[1] == '+'):
    print(int(exp[0]) +int( exp[2]))
elif(exp[1] == '-'):
     print(int(exp[0]) - int( exp[2]))
elif(exp[1] == '%'):
     print(int(exp[0]) % int( exp[2]))
elif(exp[1] == '/'):
     print(int(exp[0]) /int( exp[2]))
elif(exp[1] == '*'):
     print(int(exp[0]) *int( exp[2]))
elif(exp[1] == '**'):
     print(int(exp[0]) ** int( exp[2]))
else:
    print("Sorry i don't know that operation")