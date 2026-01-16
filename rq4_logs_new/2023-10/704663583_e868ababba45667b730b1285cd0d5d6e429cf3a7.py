import json
from savings_classes import Account, Transaction

# Function to check account balance
def check_account_balance(account):
    print(f'Owner: {account.owner}, Balance: {account.balance} {account.currency}')

# Function to check savings goal status
def check_savings_goal_status(account):
    #print(f"Account: {account.id}, Owner: {account.owner}")
    if account.savings_goal is not None:
        print(f'Savings goal: {account.savings_goal} {account.currency}')
        if account.balance >= account.savings_goal:
            print('Congratulations you have reached your saving goal! \n')
        else:
            amount_needed = account.savings_goal - account.balance
            print(f'Amount you need to reach your goal: {amount_needed} {account.currency} \n')

# Function to rate accounts by balance
def rate_accounts(accounts):
    sorted_accounts = sorted(accounts, key=lambda x: x.balance, reverse=True)
    return sorted_accounts

# Function to get account rating
def get_account_rating(accounts, account):
    sorted_accounts = rate_accounts(accounts)
    account_rank = sorted_accounts.index(account) + 1
    print(f'\nSavings Rating: {account_rank}/{len(sorted_accounts)}')


# Load and read the JSON file with the account data
with open('accounts.json') as a_f:
    accounts_data = json.load(a_f)

# Initialize a list to store account objects
accounts_list = []

# Create a dictionary to map account IDs to account objects
account_dict = {}

# Set savings goals for each account
savings_goals = {
    'acc123': 40000,
    'acc456': 35000,  
    'acc789': 10000,   
    'acc012': 15000
}

# Iterate through accounts_data to create the accounts
for i in accounts_data['accounts']:
    account = Account(
        i['id'],
        i['account_number'],
        i['account_type'],
        i['balance'],
        i['currency'],
        i['owner']
    )
    account.savings_goal = savings_goals.get(account.id) 
    accounts_list.append(account)
    account_dict[account.id] = account

# Print initial account balances before any transactions
print('\nAccount Balances:\n')
for account in accounts_list:
    check_account_balance(account)
    check_savings_goal_status(account)

# Load and read the JSON file with the transaction data
with open('transactions.json') as t_f:
    transaction_data = json.load(t_f)

# Initialize a list to store transaction objects
transaction_list = []

# Iterate through transaction_data to create the transactions and update account balances
for j in transaction_data['transactions']:
    transaction = Transaction(
        j['id'],
        j['date'],
        j['description'],
        j['amount'],
        j['currency'],
        j['account_id']
    )
    account = account_dict.get(transaction.account_id)
    if account:
        account.update_balance(transaction.amount)
    transaction_list.append(transaction)

# Print updated account balances and check savings goals
print('\n\nAccount Balances after transactions:\n')
for account in accounts_list:
    check_account_balance(account)
    check_savings_goal_status(account)

# Rate and print accounts by balance
print('\n\nRated Accounts:')
for account in rate_accounts(accounts_list):
    get_account_rating(accounts_list,account)
    check_account_balance(account)