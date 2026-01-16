import random

class Account:
    def __init__(self, name, email, address, account_type):
        self.account_number = random.randint(1000000000, 9999999999)
        self.name = name
        self.email = email
        self.address = address
        self.account_type = account_type
        self.balance = 0
        self.transaction_history = []
        self.loans = []
        self.loan_enabled = True

    def deposit(self, amount, bank):
        self.balance += amount
        bank.total_balance += amount
        self.transaction_history.append(f"Deposited: {amount}")
        print(f"Deposited {amount} successfully.")

    def withdraw(self, amount, bank):
        if bank.total_balance < amount:
            print("Bank is bankrupt. Unable to process the withdrawal.")
        elif self.balance >= amount:
            self.balance -= amount
            bank.total_balance -= amount
            self.transaction_history.append(f"Withdrew: {amount}")
            print(f"Withdrew {amount} successfully.")
        else:
            print("Withdrawal amount exceeded your balance.")

    def check_balance(self):
        print(f"Available balance: {self.balance}")
        return self.balance

    def check_transaction_history(self):
        if self.transaction_history:
            for transaction in self.transaction_history:
                print(transaction)
        else:
            print("No transactions yet.")

    def take_loan(self, amount, bank):
        if len(self.loans) < 2 and self.loan_enabled:
            self.balance += amount
            bank.total_balance += amount
            self.loans.append(amount)
            self.transaction_history.append(f"Loan taken: {amount}")
            print(f"Loan of {amount} taken successfully.")
        else:
            print("Loan limit exceeded or loans are disabled.")

    def transfer(self, amount, target_account, bank):
        if bank.total_balance < amount:
            print("Bank is bankrupt. Unable to process the transfer.")
        elif self.balance >= amount:
            self.balance -= amount
            target_account.deposit(amount, bank)
            self.transaction_history.append(f"Transferred {amount} to account {target_account.account_number}")
            print(f"Transferred {amount} to {target_account.name}")
        else:
            print("Insufficient balance for transfer")
