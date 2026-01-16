using System;

namespace BankAccounts
{
    public abstract class Account
    {
        private Customer customer;
        private decimal balance;
        private decimal interestRate;

        public Account(Customer customer, decimal balance, decimal interestRate)
        {
            this.Customer = customer;
            this.Balance = balance;
            this.InterestRate = interestRate;
        }

        public Customer Customer 
        {
            get { return this.customer; }
            set 
            {
                if (value == null)
                {
                    throw new ArgumentNullException("Bank account with no Customer defined is a no-go");
                }
                this.customer = value; 
            }
        }
        public decimal Balance 
        {
            get { return this.balance;}
            set { this.balance = value;}
        }
        public decimal InterestRate 
        {
            get { return this.interestRate;}
            set 
            {
                if (value < 0)
                {
                    throw new ArgumentException("Interest rate cannot be a negative number!");
                }
                this.interestRate = value;
            }
        }

        public virtual decimal InterestCalculations(int months)
        {
            if (months < 0)
            {
                throw new ArgumentException("Number of months cannot be a negative amount");
            }

            return months * (this.InterestRate / 100m * this.Balance);
        }

        public override string ToString()
        {
            return string.Format("Name: {0} - Address: {3} - Balance: {1:c} - iRate: {2}", this.Customer.Name, this.Balance, this.InterestRate, this.Customer.Address);
        }
    }
}