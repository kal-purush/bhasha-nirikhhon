#account.acc.Account > package - module - Class
#classes and sub class 

class Account: 

    def __init__(self, filepath):
        self.filepath=filepath
        with open(filepath, 'r') as file:
            self.balance = int(file.read())

    def withdraw(self,amount):
        self.balance = self.balance - amount

    def deposit(self,amount):
        self.balance = self.balance + amount

    def commit(self):
        with open(self.filepath, 'w') as file:
            file.write(str(self.balance))


#Inheritance
class Checking(Account):
    def __init__(self,filepath, fee):
        Account.__init__(self, filepath)
        self.fee = fee

    def transfer(self, amount):
        self.balance = self.balance - amount -self.fee

checking = Checking("balance.txt", 1)
checking.deposit(10)
checking.transfer(20)
print(checking.balance)
checking.commit()



# account = Account("balance.txt")
# print(account.balance)
# account.withdraw(100)
# print(account.balance)
# account.deposit(500)
# print(account.balance)
# account.commit()

#OOP Based Account managing appication
# class
# object instance
# instance variable
# class variable
# doc strings
# data member
# constrctor
# methods
# instantiation
# inheritance
# Attributes