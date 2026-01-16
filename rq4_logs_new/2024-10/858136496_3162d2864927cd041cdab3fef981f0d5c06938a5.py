class BankAccount:
    def __init__(self, account_number, customer_name, date_of_opening, balance=0):
        self.account_number = account_number
        self.customer_name = customer_name
        self.date_of_opening = date_of_opening
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount
        return f"Ksh {amount} has been deposited. New balance is: Ksh {self.balance:.2f}"

    def withdraw(self, amount):
        if self.balance < amount:
            return "Insufficient balance!"
        else:
            self.balance -= amount
            return f"Ksh {amount} has been withdrawn. New balance is: Ksh {self.balance:.2f}"

    def check_balance(self):
        print(f"Current balance: Ksh {self.balance:.2f}")

    def customer_details(self):
        print(f"Customer Name: {self.customer_name}")
        print(f"Account Number: {self.account_number}")
        print(f"Date of Opening: {self.date_of_opening}")
        print(f"Current Balance: Ksh {self.balance:.2f}")


account = BankAccount(account_number="915457234", customer_name="susan", date_of_opening="2024-10-15", balance=20000)


print(account.deposit(5000))   # Depositing 5000


print(account.withdraw(2000))  # Withdrawing 2000
print(account.withdraw(45000)) # Trying to withdraw more than the balance


account.check_balance()


account.customer_details()