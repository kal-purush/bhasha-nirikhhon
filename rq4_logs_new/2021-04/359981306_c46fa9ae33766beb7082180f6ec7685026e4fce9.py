class Bank(object):
    def __init__(self, savingAccount, currentAccount, name, location):
         self.savingAccount = savingAccount
         self.currentAccount = currentAccount
         self.name = name
         self.location = location
        
    def savings(self):
        print(f"You have {self.savingAccount} in your savings account")
    
    def Name(self):
        print(f"The name of this bank is {self.name}")

    def Location(self):
        print(f"This bank is located at {self.location}")

bank = Bank("$500", "$200", "Bank", "Everywhere")
bank.savings()
bank.Name()
bank.Location()