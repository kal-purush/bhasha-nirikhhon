'''Module to perform basic Banking operations '''

import time,pickle,os

path0 = "Data\\"
process = True        



class Customer :
    no_of_customers = 0

    def __init__(self):

        Customer.no_of_customers +=1

        self.CustomerData = None 
        self.Name  = None
        self.Password = None
        self.Address = None
        self.DOB = None
        self.Email = None
        self.Phno = None
        self.info = None
        self.amount = 0
        
        self.EMI = None    
        self.job = None
        self.max_loan_amt = None
        self.age = None
        self.loantype= None
    
    def InputData(self,lst) :
        
        self.Name  = lst[0]
        self.Password = lst[1]
        self.Address = lst[2]
        self.DOB = lst[3]
        self.Email = lst[4]
        self.Phno = lst[5]
        self.age = lst[6]
        self.amount = 0

    def Withdraw(self,amount):
        if self.amount  >= amount  :
            
            self.amount = self.amount - amount

            print "Capital left after Withdrawing is - " , self.amount

        else  :
            print "Sorry, your account does not have enough credit to withdraw the given amount"

    def Deposit(self,amount):
        self.amount =  self.amount + amount
        print "Updated amount - " , self.amount

    def ViewData(self):
        print "Name :  " , self.Name
        print "Address : " ,self.Address
        print "Date of Birth : " ,self.DOB
        print "Email ID  :  " , self.Email
        print "Phone number :  " , self.Phno
        print "Credit : ",self.amount
        print "Age : ",self.age
        print " "
        if self.loantype != None  :
            print "Loan Details "
            print "Loantype : ",self.loantype
            print "EMI for loan: ",self.EMI
            print "Job Sector : ",self.job
            print "Current loan amount :  ",self.max_loan_amt
            print " " 

            

    def Changepass(self):
        print "Changing Password. Enter original password: "
        testpass = raw_input("")
        if testpass == self.Password :
            newpass = raw_input("Enter new password : ")
            confnewpass= raw_input("Confirm the password: " )

            if newpass == confnewpass  :
                self.Password = newpass
                print "Password Successfully changed. It is: " , self.Password
            else :
                print "Passwords do not match.Try again"
                self.Changepass()
    
    

    def ChangeData(self):
        print "What do you want to change? "
        print "0.Exit \n 1.Password \n 2.Address \n 3.DOB \n 4.Email \n 5.Phone number"
        prompt = int(raw_input("Enter option: " ))

        if prompt == 0 :
            pass

        elif prompt == 1 :
            self.Changepass()

        elif prompt == 2 :
            add= raw_input("Enter new address : ")
            self.Address = add
            print "Changed Address is ",self.Address
        elif prompt == 3 :
            db= raw_input("Enter new Date of birth : ")
            self.DOB = db
            print "The date of birth has been changed to ",self.DOB
        elif prompt ==4 :
            em= raw_input("Enter new Email ID : ")
            self.Email = em
            print "The Email ID has been changed to ",self.Email
        elif prompt == 5:
            ph= raw_input("Enter new phone number : ")
            self.Phno = ph
            print "The phone number has been changed to ",self.Phno
        else:
            print "Provide a valid option "

    def update(self):
        path = "Data\\"
        
        Foo = file(path+self.Name,'wb')
        pickle.dump(self,Foo)
        Foo.close()
        
        
#contains mostly static methods        
class Bank :
    txt  = "Banking Operations  \n 1.View Details \n 2.Withdraw \n 3.Deposit \n 4.View Amount \n 5.Change Credentials \n 6.Transfer Capital \n 7.Loan  "
    txtcont = " 8.View Interest rate \n 9.Save and exit "

    prompt1 = "Enter Amount to be Withdrawn : "
    prompt2 = "Enter Amount to be Deposited : "
    


    @staticmethod
    def menu(instance) :
        
        while True:
            print "  "
            print Bank.txt
            print Bank.txtcont

            
            prompt = int(raw_input("Enter Option : "))
            if prompt ==1 :
                instance.ViewData()                
            elif prompt == 2 :
                amt = float(raw_input(Bank.prompt1))
                instance.Withdraw(amt)
            elif prompt == 3 :
                amt1 = float(raw_input(Bank.prompt2))
                instance.Deposit(amt1)
            elif prompt == 4:
                print "Available Amount : " , instance.amount
            elif prompt == 5 :
                instance.ChangeData()
            elif prompt == 6:
                                
                tcash = float(raw_input("Enter amount to be transferred :"))
                sname = raw_input("Enter name of the receiver : ")                
                Bank.TransferCash(instance,sname,tcash)

            elif prompt == 7 :
                print "Entering loan menu....."
                Bank.loan(instance)

            elif prompt == 8 :
                print "Our bank currently provides everyone an interest rate of 8% per annum "
                print "Your interest for the current amount inn the bank @8 % per annum : ",self.amount * 0.08
                print " " 
                
            elif prompt == 9:
                instance.update()
                break
                exit()

            

    @staticmethod
    def TransferCash(sendobj,name,cash):
        path = "Data\\"

        recf = open(path + name,'rb')
        recobj = pickle.load(recf)
        #transfers capital from one account to another
        if sendobj.amount >= cash :
            sendobj.amount = sendobj.amount - cash
            recobj.amount = recobj.amount + cash 
            #updates the amount to the respective files
            sendobj.update()
            recobj.update()

            print "Amount ",cash," has been transferred from ",sendobj.Name," to ",recobj.Name
            
    @staticmethod
    def loan(instance):
    
        Loantype = None
        print"1-Personal Loan\n2-House Loan"
        opt = int(raw_input("Enter preferred option :"))
        if opt not in range(1,3):
            print "Invalid Selection"
            Bank.loan()                                                  #enter the arguement as object

        elif opt == 1:
            print "You have opted for personal loan"
            Loantype = "personal"
            instance.loantype = Loantype
            age = int(raw_input("Enter your age :"))
            annual_income = int(raw_input("Enter your annual income (deducting tax) :"))
            job = str(raw_input("Enter the sector you work in (Government or Private) :"))
            if job.lower() == "government":
                max_loan_amt = (annual_income*5)+(0.3*annual_income)
                print "You are eligible for a loan amount of",max_loan_amt
            elif job.lower() == "private":
                max_loan_amt = (annual_income*5)+(0.2*annual_income)
                print "You are eligible for a loan amount of",max_loan_amt

            req = int(raw_input("Are you in the requirement of the complete loan amount(choose - 1)or do you want to reduce it(choose - 2) :"))
            if req == 1:
                print "Now you have choosen the loan scheme for an amount",max_loan_amt
                Bank.time_repay_personal(job,max_loan_amt,instance)
                

            elif req == 2:
                print "Enter the preferred loan amount"
                print "Lesser than ",max_loan_amt
                loan_req = int(raw_input("Enter the amount :"))
                if loan_req <= max_loan_amt:
                    max_loan_amt = loan_req
                    print "Loan amount acceptable\nYou have choosen the loan scheme for an amount",max_loan_amt
                    Bank.time_repay_personal(job,max_loan_amt,instance)
                else:
                    print "Loan amount greater than maximum loan amount allowed"
            else:
                print "invalid selection,session cancelled"
                Bank.loan()
        elif opt == 2:
            Loantype = "House"
            instance.loantype = Loantype
            print "You have opted for House loan"
            age = int(raw_input("Enter your age :"))
            annual_income = int(raw_input("Enter your annual income (deducting tax) :"))
            job = str(raw_input("Enter the sector you work in (Government or Private) :"))
            if job.lower() == "government":
                max_loan_amt = (annual_income*10)+(0.325*annual_income)
                print "You are eligible for a loan amount of",max_loan_amt
            elif job.lower() == "private":
                max_loan_amt = (annual_income*10)+(0.285*annual_income)
                print "You are eligible for a loan amount of",max_loan_amt

            req = int(raw_input("Are you in the requirement of the complete loan amount(choose - 1)or do you want to reduce it(choose - 2) :"))
            if req == 1:
                print "Now you have choosen the loan scheme for an amount",max_loan_amt
                Bank.time_repay_house(job,max_loan_amt,instance)
            
                

            elif req == 2:
                print "Enter the preferred loan amount"
                print "Lesser than ",max_loan_amt
                loan_req = int(raw_input("Enter the amount :"))
                if loan_req <= max_loan_amt:
                    max_loan_amt = loan_req
                    print "Loan amount acceptable\nYou have choosen the loan scheme for an amount",max_loan_amt
                    Bank.time_repay_house(job,max_loan_amt,instance)
                else:
                    print "Loan amount greater than maximum loan amount allowed"

                    
       
    @staticmethod             
    def time_repay_personal(job,max_loan_amt,instance):
        print "Now you have chosen your loan amount\nNow to decide the time period of the loan"
        if job.lower() == "private" or instance.age>35:
            print "The time period of the loan will be over a period of 5 years"
            total_amount_repay = (max_loan_amt) + (max_loan_amt)*0.75
            EMI = (total_amount_repay)/500
            print "Your monthly installment value will be",EMI
        elif job.lower() == " government" or instance.age<35:
            print "The time period of the loan will be over a period of 8 years"
            total_amount_repay = (max_loan_amt)+(max_loan_amt)*0.50
            EMI = (total_amount_repay)/800
            print "Your monthly installment will be",EMI
            
        instance.EMI = EMI
        instance.job = job
        instance.max_loan_amt = max_loan_amt
        instance.update()
            
            
            
    @staticmethod        
    def time_repay_house(job,max_loan_amt,instance):
        print "Now you have chosen your loan amount\nNow to decide the time period of the loan"
        if job.lower() == "private" or instance.age>35:
            print "The time period of the loan will be over a period of 15 years"
            total_amount_repay = (max_loan_amt) + (max_loan_amt)*0.75
            EMI = (total_amount_repay)/565
            print "Your monthly installment value will be",EMI
        elif job.lower() == " government" or instance.age<35:
            print "The time period of the loan will be over a period of 18 years"
            total_amount_repay = (max_loan_amt)+(max_loan_amt)*0.50
            EMI = (total_amount_repay)/550
            print "Your monthly installment will be",EMI
            
        instance.EMI = EMI
        instance.job = job
        instacne.max_loan_amt = max_loan_amt
        instance.update()
        
def opt(custObj):
    global process
    option = int(raw_input("Enter 1 to enter menu or 0 to exit : "))
    if option == 1 :
        Bank.menu(custObj)
    elif option == 0 :
        process  =False
    else :
        print "Invalid option try again "
        opt(custObj)

        
def LoginPage():
    global process

    print 'Welcome to our Bank - We care '
    print '' 
    txt =  '''Select 1 - Existing account,
Select 2 - Create new account
Select 3 - Exit : '''
    try : 
        x = int(raw_input(txt))

        if x == 1 :
            SignIn()
        if x == 2 :
            SignUp()
        if x == 3 :
            process = False
            

    except :
        print "Error - Invalid"
        
        LoginPage()

def transferData(obj):
    path = 'Data\\' + obj.Name
    f = file(path, 'wb')
    
    pickle.dump(obj,f)
    f.close()

def dateError(date) :
    #DD/MM/YYYY
    if date[2] != "/" or date[5] != "/":
        print "Date not in correct format"
        SignUp()
    elif int(date[0:2]) not in range(1,32):
        print "Wrong Date Type "
        SignUp()
    elif int(date[3:5]) not in range(1,13):
        print "Wrong Date Type"
        SignUp()

    elif int(date[6:10]) not in range(1940,2001):
        print "Not in age limit"
        SignUp()
    elif date == " ":
        print "Date can't be blank"
        SignUp()
    else:
        pass

    
def emailError(email) :
    #q = ajsdfgasjdf
    q1 = (email[-4:-1] + email[len(email)-1])
    q2 =(email[-3:-1] + email[len(email)-1])
    if '@' not in email :
        print "Invalid email ID  "
        SignUp()

    elif q1 != ".com" and q2 != ".in" :
        
        
        print "Invalid email ID "
        SignUp()

   
    elif len(email) == 0   :
        print "Invalid email ID "
        SignUp()

        
    else :
        pass
        
    
def phoneError(phone) :
    for i in phone :
        if int(i) not in range(0,10):
            print "Invalid phone number1"
            SignUp()
            break
    if len(phone) != 10:
        print "Invalid Phone Number len"
        SignUp()

    
    
    
def SignUp() :
    
    print "\\ Create New Account //"
    Data = []
    obj = Customer()
    
    Username  = raw_input("Enter your Username(cannot be changed) : ")
    if len(Username) == 0:
        print "Username can't be blank"
        SignUp()
        
    Password = raw_input("Enter your Password : ")
    if len(Password) == 0:
        print "Password can't be blank"
        SignUp()
    RePassword = raw_input("Re-Enter your Password : ")
        
    if Password != RePassword :
        print " "
        print "Passwords do not match "
        print " "
        SignUp()
                
    Address = raw_input("Enter your current address : ")
    if Address == " ":
        print "Address can't be blank"
        SignUp()
    
    DOB = raw_input("Enter Date of Birth (DD/MM/YYYY) : ")
    dateError(DOB)
    
    Email = raw_input("Enter a valid Email ID : ")
    emailError(Email)
    Phno = raw_input("Enter your Phone number : ")
    phoneError(Phno)
    
        
    Age = raw_input("Enter your age : ")
    if int(Age) not in range(18,77):
        print "Invalid age"
        SignUp()
        

   
    Data.append(Username)
    Data.append(Password)
    Data.append(Address)
    Data.append(DOB)
    Data.append(Email)
    Data.append(Phno)
    Data.append(Age)

    obj.InputData(Data)
    transferData(obj)
    print ""

    print " SignUp complete " 
    



#def errorCheck():
    
    
def SignIn():

    print "Sign in to your account "
    print ""
    Username = raw_input("Enter your username  : ")
    Password = raw_input("Enter your Password : ")
    print " Logging in .  "
    print ""
    time.sleep(1)
    print ".",
    time.sleep(1)
    print ".",


    try : 
     
        path = "Data\\" + Username
        f = open(path,'rb')
        custObj = pickle.load(f)


        if Username == custObj.Name and Password == custObj.Password:
            print "Logged in "
            
        else :
            print " Invalid Username/Password "
            SignIn()
    
    except :
        print " Invalid Username/Password "
        SignIn()
        

    
    time.sleep(1)

    opt(custObj)
    
#main        
while process: 
    LoginPage()
    
    
            