user_a=input("Username : ")
pass_a=input("Password : ")
if user_a=="admin" and pass_a=="1234":
    print("Login success!!")
    print("---Abc Shop---")
    print("1.Lemon  : 10 THB")
    print("2.Orange : 15 THB")
    print("3.Banana : 8  THB")
    print("4.Exit")
    pick_a=int(input("Number : "))
    amount_a=int(input("Amount : "))
    p1=10;p2=15;p3=8
    if pick_a==1:
        total_a=p1*amount_a
        print("Price : ",total_a)
    elif pick_a==2:
        total_a=p2*amount_a
        print("Price : ",total_a)
    elif pick_a==3:
        total_a=p3*amount_a
        print("Price : ",total_a)
    else :
        print("Exit")
else :
    print("Username or Password is wrong Please contract Admin")