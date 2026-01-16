using System;
using System.Collections.Generic;

namespace classes
{
    public class BankAccount
    {
        private List<Transaction> allTransactions = new List<Transaction>();

        private static int accountNumberSeed = 1234567890;
        public string Number { get; }
        public string Owner { get; set; }
        public decimal Balance
        {
            get
            {
                decimal balance = 0;
                foreach (var itm in allTransactions)
                {
                    balance += itm.Amount;
                }

                return balance;
            }
        }

        /// <summary>
        /// This constructor is used to open a new bank account
        /// </summary>
        /// <param name="name"></param> Account owner
        /// <param name="initialBalance"></param> Starting balance
        public BankAccount(string name, decimal initialBalance)
        {
            this.Number = accountNumberSeed.ToString();
            accountNumberSeed++;
            this.Owner = name;
            MakeDeposit(initialBalance, DateTime.Now, "Initial balance");
        }

        /// <summary>
        /// This method is used to make bank deposits
        /// </summary>
        /// <param name="amount"></param> Amount being deposited
        /// <param name="date"></param> Date of deposit
        /// <param name="note"></param> Any notes regarding the deposit
        public void MakeDeposit(decimal amount, DateTime date, string note)
        {
            if (amount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(amount), "Amount of deposit must be positive.");
            }

            var deposit = new Transaction(amount, date, note);
            allTransactions.Add(deposit);
        }

        /// <summary>
        /// This method is used to make bank withdrawals
        /// </summary>
        /// <param name="amount"></param> Amount being withdrawn
        /// <param name="date"></param> Date of withdrawal
        /// <param name="note"></param> Any notes regarding the withdrawal
        public void MakeWithdrawal(decimal amount, DateTime date, string note)
        {
            if (amount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(amount), "Amount of withdrawal must be positive.");
            }

            if (Balance - amount < 0)
            {
                throw new InvalidOperationException("Insufficient funds for this withdrawal.");
            }

            var withdrawal = new Transaction(-amount, date, note);
            allTransactions.Add(withdrawal);
        }

        /// <summary>
        /// This method returns the transaction history
        /// </summary>
        /// <returns></returns>
        public string GetAccountHistory()
        {
            var report = new System.Text.StringBuilder();

            decimal balance = 0;

            report.AppendLine("Date\t\tAmount\tBalance\tNote");
            foreach (var itm in allTransactions)
            {
                balance += itm.Amount;
                report.AppendLine($"{itm.Date.ToShortDateString()}\t{itm.Amount}\t{balance}\t{itm.Notes}");
            }

            return report.ToString();
        }
    }
}