# I've not implemented the loop and menu to choose options ;
# This is to understand the OOP Concepts like abstract classes implementation.

from abc import ABCMeta, abstractmethod
from random import randint
class SwissBank(metaclass=ABCMeta):
    @abstractmethod
    def deposit(self):
        return 0
    @abstractmethod
    def createAccount(self):
        return 0
    @abstractmethod
    def deposit(self):
        return 0
    @abstractmethod
    def withdraw(self):
        return 0
    @abstractmethod
    def balEnquiry(self):
        return 0

class SavingsAccount(SwissBank):
    account_balance = 0.0
    account_number = 0

    def __init__(self):
        print("Savings Account initialised")
        self.account_number = randint(10000, 99999)

    def createAccount(self):
        return 0
        
    def deposit(self, ammount):
        self.account_balance += ammount

    def withdraw(self, ammount):
        self.account_balance -= ammount

    def balEnquiry(self):
        print("Your available balance is : {}".format(self.account_balance))



class Accounts:
    account_list = []

    def add_acc(self, SavingsAccount):
        self.account_list.append(SavingsAccount)

    def close_acc(self, acc_no):
        print("Account {} closed successfully".format(self.account_list.remove(acc_no)))

    def display_acc(self):
        print("Below is the accounts avilable ")
        for a in self.account_list:
            print("Account number is : "+str(a.account_number)+" Balance is : "+str(a.account_balance))




acc = Accounts()
sav1 = SavingsAccount()
sav2 = SavingsAccount()
sav1.deposit(230)
acc.add_acc(sav1)
acc.add_acc(sav2)
acc.display_acc()
sav1.withdraw(30)
acc.close_acc(acc.account_list[1])
sav1.balEnquiry()
acc.display_acc()