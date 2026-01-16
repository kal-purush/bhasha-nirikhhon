import sqlite3
import time

class Wallet:
    def __init__(self, user_id):  # Use user_id instead of username
        self.user_id = user_id
        self.balance = self.get_balance_from_db()

    def get_balance_from_db(self):
        """Fetches the wallet balance for the user from the database."""
        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM wallet WHERE user_id = ?", (self.user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0.0  # Return balance or default 0.0

    def update_balance(self, amount):
        """Updates the user's wallet balance in the database."""
        conn = sqlite3.connect("freelancer_marketplace.db")
        cursor = conn.cursor()
        cursor.execute("UPDATE wallet SET balance = balance + ? WHERE user_id = ?", (amount, self.user_id))
        conn.commit()
        conn.close()

    def deposit(self, amount):
        if amount > 0:
            self.update_balance(amount)
            print("\nSuccessfully deposited!")
            time.sleep(1.5)
        else:
            print("Invalid deposit amount!")
            time.sleep(1.5)

    def withdraw(self, amount):
        if amount > self.balance:
            print("\nInsufficient funds!")
            time.sleep(1.5)
        elif amount > 0:
            self.update_balance(-amount)
            print(f"Successfuly Withdrawed!")
            time.sleep(1.5)
        else:
            print("Invalid withdrawal amount!")
            time.sleep(1.5)

class Payment:
    def __init__(self, amount, milestone):
        self.amount = amount
        self.milestone = milestone
        self.status = "Pending"

    def release_payment(self, employer_wallet, freelancer_wallet):
        """Transfers funds from employer to freelancer."""
        if employer_wallet.get_balance_from_db() >= self.amount:
            employer_wallet.withdraw(self.amount)
            freelancer_wallet.deposit(self.amount)
            self.status = "Paid"
            print(f"Php {self.amount} transferred for milestone '{self.milestone}'")
        else:
            print("Insufficient funds to release payment!")

