using System;

namespace practice_05._25._19_interface.Task2_BankAccount
{
    public class BankAccount
    {
        protected decimal currentBalance;

        public string AccountOwner { get; }

        public BankAccount(decimal currentBalance, string balanceOwner)
        {
            this.currentBalance = currentBalance;
            this.AccountOwner = balanceOwner;
        }

        public decimal CurrentBalance
        {
            get
            {
                return currentBalance;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                else
                    currentBalance = value;
            }
        }

        public decimal CloseAccount()
        {
            decimal returnedBalance = this.currentBalance;
            this.currentBalance = 0;

            return returnedBalance;
        }
    }
}