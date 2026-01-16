using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class CreditCardAccount
    {
        //FIELDS
        private int _accountNumber;
        private Customer _customerInfo;
        private decimal _balance;
        private bool _isPastDue;
        private decimal _annualInterestRate;

        //PROPERTIES
        public int AccountNumber
        {
            get { return _accountNumber; }
            set { _accountNumber = value; }
        }
        public Customer CustomerInfo
        {
            get { return _customerInfo; }
            set { _customerInfo = value; }
        }
        public decimal Balance
        {
            get { return _balance; }
            set { _balance = value; }
        }
        public bool IsPastDue
        {
            get { return _isPastDue; }
            set { _isPastDue = value; }
        }
        public decimal AnnualInterestRate
        {
            get { return _annualInterestRate; }
            set { _annualInterestRate = value; }
        }

        //CONSTRUCTORS
        public CreditCardAccount() { }

        public CreditCardAccount(int accountNumber, Customer customerInfo, decimal balance, bool isPastDue,
            decimal annualInterestRate)
        {
            AccountNumber = accountNumber;
            CustomerInfo = customerInfo;
            Balance = balance;
            IsPastDue = isPastDue;
            AnnualInterestRate = annualInterestRate;
        }

        //METHODS
        public override string ToString()
        {
            //return base.ToString();

            return string.Format("\n-=-= Credit Card Account =-=-\n" +
                "Account Number: {0}\t\tIs Past Due? {1}\n{2}\n" +
                "Balance: {3:c}\t\tAnnual Interest Rate: {4:p0}",
                AccountNumber, IsPastDue, CustomerInfo,
                Balance, AnnualInterestRate);
        }
    }
}