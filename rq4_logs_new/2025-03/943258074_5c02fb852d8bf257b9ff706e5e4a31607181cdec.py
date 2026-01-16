class Wallet:
    def __init__(self):
        self.balance = 0.0
    
    def deposit(self, amount):
        if amount > 0:
            self.balance += amount
            print(f"Deposited ${amount}. New balance: ${self.balance}")
        else:
            print("Invalid deposit amount!")

    def withdraw(self, amount):
        if amount > self.balance:
            print("Insufficient funds!")
        elif amount > 0:
            self.balance -= amount
            print(f"Withdrawn ${amount}. New balance: ${self.balance}")
        else:
            print("Invalid withdrawal amount!")

class Payment:
    def __init__(self, amount, milestone):
        self.amount = amount
        self.milestone = milestone
        self.status = "Pending"
    
    def release_payment(self, employer_wallet, freelancer_wallet):
        if employer_wallet.balance >= self.amount:
            employer_wallet.withdraw(self.amount)
            freelancer_wallet.deposit(self.amount)
            self.status = "Paid"
            print(f"Payment of ${self.amount} released for {self.milestone}")
        else:
            print("Employer has insufficient funds to release payment!")