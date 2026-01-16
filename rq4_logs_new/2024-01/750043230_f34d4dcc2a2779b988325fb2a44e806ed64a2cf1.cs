using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankAccount
{
    /// <summary>
    /// Represents a single customers bank account
    /// </summary>
    public class Account
    {
        /// <summary>
        /// Creates an account with a specific owner and a balance of 0
        /// </summary>
        /// <param name="accOwner">Full name of the customer who owns the account</param>
        public Account(string accOwner)
        {
            Owner = accOwner;
        }
        /// <summary>
        /// Full name od the owner of the account 
        /// </summary>
        public string Owner { get; set; }

        /// <summary>
        /// How much money is currently in the account
        /// </summary>
        public double Balance { get; private set; }

        /// <summary>
        /// Add a specified amount of money to the account
        /// </summary>
        /// <param name="amt">positive amount to deposit</param>
        public void Deposit(double amt)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Take a specific amount of money from the account
        /// </summary>
        /// <param name="amt">Positive amount to withdraw</param>
        public void Withdraw(double amt)
        {
            throw new NotImplementedException();
        }

    }
}