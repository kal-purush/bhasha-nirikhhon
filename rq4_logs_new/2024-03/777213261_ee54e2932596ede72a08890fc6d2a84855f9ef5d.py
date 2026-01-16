import math
### investment = " investment calculation is the amount of interest you will earn on your investment"
### bond = " bond calculation is to calculate the amount you will have to pay on a home loan"
### taking input from user which calculation you want to do invesment or bond
inv_bond = input(str("Please select option which calculation you want to see Investment or Bond :" )).lower()
if inv_bond == "investment":
    initial_amt = float (input("please enter the amount of money you are depositing:  "))
    int_rate = int (input("please enter the rate of interest: "))
    r = (int_rate/100)
    time = float(input(  " please enter the no of years you want to invest: "))  
    interest = str(input("select either 'simple' or 'compound' interest: ")).lower()
    if interest == "simple":
        "simple" == interest
        interest = initial_amt*(1+r*time)
        total = interest
        print(f"Interest earned over {time} years : {total:.2f}".format())
    elif interest == "compound":
          "compound"== interest
          interest = initial_amt * math.pow((1+r), time)
          total  = interest
          print(f"Interest earned over {time} years : {total:.2f}".format())  
#### Bond in another condition
elif inv_bond == "bond":
     present_value = float(input("please enter the present value of the house:  "))
     intr = int (input("please enter the rate of interest: "))
     r = (intr/100)/12
     months = float(input(" please enter the no of months bond will be repaid: "))
     repayment = (r*present_value)/(1-(1+r)**(-months))
     print(f"repayment : {repayment:.2f}".format())
                