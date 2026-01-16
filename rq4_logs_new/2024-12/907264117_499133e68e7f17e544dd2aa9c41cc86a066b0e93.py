print('Welcome to the biling system !\n')
n = int(input('Please enter the number of units you have consumed: '))
k = 1
count=0
while(k<=n):
    p = int(input(f'Enter the price of {k} item: \n'))
    count+=p
    k+=1
print("Your total is: ", count) 
customer = str(input("Are you a new customer or an old customer ?\n"))
if(customer == 'new' or customer == 'New' or customer == 'NEW'):
    print("You are eligible for a 10% discount")
    print("Your total is: ", count - (count*0.1))
elif(customer == 'old' or customer == 'Old' or customer == 'OLD'):
    print("You are eligible for a 20% discount")
    print("Your total is: ", count - (count*0.2))
else:
    print('INVALID INPUT !')
print("Thank you for shopping with us !")
    