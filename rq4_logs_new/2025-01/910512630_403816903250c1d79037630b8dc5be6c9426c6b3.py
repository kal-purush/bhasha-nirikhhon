#Expense Tracker Python Code
import json
from datetime import datetime

FILE_NAME = "expenses.json"

# Load expenses from file
def load_expenses():
    try:
        with open(FILE_NAME, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# Save expenses to file
def save_expenses(expenses):
    with open(FILE_NAME, "w") as file:
        json.dump(expenses, file, indent=4)

# Add a transaction
def add_transaction(expenses):
    transaction_type = input("Enter transaction type (income/expense): ").strip().lower()
    if transaction_type not in ["income", "expense"]:
        print("Invalid transaction type! Use 'income' or 'expense'.")
        return
    amount = input("Enter amount: ").strip()
    try:
        amount = float(amount)
        if transaction_type == "expense":
            amount = -abs(amount)  # Ensure expenses are negative
    except ValueError:
        print("Invalid amount! Please enter a number.")
        return
    category = input("Enter category: ").strip()
    date = input("Enter date (YYYY-MM-DD) or press Enter for today: ").strip()
    if not date:
        date = datetime.today().date().isoformat()
    else:
        try:
            date = datetime.strptime(date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            print("Invalid date format! Use YYYY-MM-DD.")
            return
    expenses.append({"type": transaction_type, "amount": amount, "category": category, "date": date})
    print("Transaction added successfully!")

# View transactions
def view_transactions(expenses):
    if not expenses:
        print("\nNo transactions to show!")
    else:
        print("\nTransaction History:")
        for idx, expense in enumerate(expenses, 1):
            print(f"{idx}. {expense['date']} | {expense['type']} | {expense['category']} | {expense['amount']}")

# Calculate balance
def calculate_balance(expenses):
    balance = sum(expense["amount"] for expense in expenses)
    print(f"\nCurrent Balance: {balance}")

# Filter transactions by category
def filter_transactions(expenses):
    category = input("Enter category to filter by: ").strip()
    filtered = [expense for expense in expenses if expense["category"].lower() == category.lower()]
    if not filtered:
        print(f"\nNo transactions found in category: {category}")
    else:
        print(f"\nTransactions in category '{category}':")
        for idx, expense in enumerate(filtered, 1):
            print(f"{idx}. {expense['date']} | {expense['type']} | {expense['amount']}")

# Main menu
def main():
    expenses = load_expenses()
    while True:
        print("\nExpense Tracker")
        print("1. Add Transaction")
        print("2. View Transactions")
        print("3. Calculate Balance")
        print("4. Filter by Category")
        print("5. Exit")
        choice = input("Choose an option: ").strip()
        if choice == "1":
            add_transaction(expenses)
        elif choice == "2":
            view_transactions(expenses)
        elif choice == "3":
            calculate_balance(expenses)
        elif choice == "4":
            filter_transactions(expenses)
        elif choice == "5":
            save_expenses(expenses)
            print("Goodbye!")
            break
        else:
            print("Invalid choice! Please try again.")

if __name__ == "__main__":
    main()
