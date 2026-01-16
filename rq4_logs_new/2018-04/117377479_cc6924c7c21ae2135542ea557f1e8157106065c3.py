class BankAccount(object):
  balance = 0
  def __init__(self, name):
    self.name = name

  def __repr__(self):
    return "This account belongs to %s and has $%.2f balance" %(self.name, self.balance)

  def show_balance(self):
    print "Balance: $%.2f" %(self.balance)

  def deposit(self, amount):
    if amount <= 0:
      print "Error, amount must be higher"
      return
    else:
      print "Amount Deposited: $%.2f" %(amount)
      self.balance += amount
      self.show_balance()

  def withdraw(self, amount):
    if amount > self.balance:
      print "Error, amount too large"
      return
    else:
      print "Amount Withdrawn: $%.2f" %(amount)
      self.balance -= amount
      self.show_balance()

my_account = BankAccount("Arshad")
print my_account
print my_account.show_balance()
my_account.deposit(2000)
my_account.withdraw(1000)
print my_account.show_balance()