class operations:
    def __init__(self,user, status=False):
        self.user=user
        self.status=status
        
    def signup(self):
        print('enter details')
        email = input('enter email:  ')
        print(email)
        password = input('enter password:  ')
        print(password)

    def login(self,status):
        if self.status == status:
            print ("you have logged out")
            print(self.status)
        else:
            print ("You have logged in")
            print(self.status)

        return self.status
   
        
        

oper = operations('admin',False)
oper.login(False)    
oper.signup()
oper = operations('admin',False)
oper.login(True)   