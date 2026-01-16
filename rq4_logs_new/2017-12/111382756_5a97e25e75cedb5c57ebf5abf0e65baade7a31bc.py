'''Module to perform basic Banking operations '''

import time,pickle,os

path0 = "Data\\"


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
    
    def InputData(self,lst) :
        
        self.Name  = lst[0]
        self.Password = lst[1]
        self.Address = lst[2]
        self.DOB = lst[3]
        self.Email = lst[4]
        self.Phno = lst[5]
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
    txt  = "Banking Operations  \n 1.View Details \n 2.Withdraw \n 3.Deposit \n 4.View Amount \n 5.Change Credentials \n 6.Transfer Capital \n 7.Save and Exit  "
    prompt1 = "Enter Amount to be Withdrawn : "
    prompt2 = "Enter Amount to be Deposited : "
    


    @staticmethod
    def menu(instance) :
        
        while True:
            print Bank.txt
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
                
            elif prompt == 7:
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
                    
                
def LoginPage():

    print ' Welcome to our Bank - We care '
    print '' 
    txt =  '''Select 1 - Existing account,
Select 2 - Create new account : '''

    x = int(raw_input(txt))
    if x not in range(1,3):
        print 'Invalid Option '
        LoginPage()

    if x == 1 :
        SignIn()
    if x == 2 :
        SignUp()

def transferData(obj):
    path = 'Data\\' + obj.Name
    f = file(path, 'wb')
    
    pickle.dump(obj,f)
    f.close()
    
def SignUp() :
    
    print "\\ Create New Account //"
    Data = []
    obj = Customer()
    
    Username  = raw_input("Enter your Username(cannot be changed) : ")
    Password = raw_input("Enter your Password : ")      
    RePassword = raw_input("Re-Enter your Password : ")
    Address = raw_input("Enter your current address : ")
    DOB = raw_input("Enter Date of Birth (DD/MM/YYYY) : ")
    Email = raw_input("Enter a valid Email ID : ")
    Phno = raw_input("Enter your Phone number : ")

    if Password == RePassword :
        Data.append(Username)
        Data.append(Password)
        Data.append(Address)
        Data.append(DOB)
        Data.append(Email)
        Data.append(Phno)

        obj.InputData(Data)
        transferData(obj)
        print ""

        print " SignUp complete " 
        
    
    else :
        print " Passwords do not match \n "
        Sign_Up()


def SignIn():

    print "Sign in to your account "
    print ""
    Username = raw_input("Enter your username  : ")
    Password = raw_input("Enter your Password : ")
    print " Logging in .  "
    print ""
##    time.sleep(1)
##    print ".",
##    time.sleep(1)
##    print ".",


    try : 
     
        path = "Data\\" + Username
        f = open(path,'rb')
        custObj = pickle.load(f)

        if Username == custObj.Name and Password == custObj.Password:
            print "Logged in "
            
        else :
            print " Inavlid Username/Password "
            SignIn()
    
    except :
        print " Inavlid Username/Password "
        SignIn()

    
    time.sleep(1)

    opt = int(raw_input("Enter 1 to enter menu or 0 to exit : "))
    if opt == 1 :
        Bank.menu(custObj)
    elif opt == 0 : 
        exit()
    else :
        print "Invalid option try again "

x  = Customer()    

            