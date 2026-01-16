using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApteanEdgeBank
{
    public abstract class Account
    {
        public int accountID;
        public DateTime dateOpened;
        public DateTime dateClosed;
        public double Balance;

        public abstract void Create(int ID,DateTime open,double balance);
        public abstract void Deposit(double amount);
        public void Withdraw(double amount)
        {
            Balance = Balance - amount;
            if (Balance > 0)
                Console.WriteLine("The new balance={0}", Balance);
            else
                Console.WriteLine("You're account balance is too low!");
        }
        public void Close()
        {
            try
            {
                Balance = 0;
                dateClosed = DateTime.Now;
                Console.WriteLine("Account closed on {0}", dateClosed.ToShortDateString());
            }
            catch (InvalidOperationException e)
            {
                Console.WriteLine("Balance is greater than 0: {0}", e);
            }
        }
        
    }

    public class ChequingAccount : Account
    {
 
        public override void Create(int ID,DateTime open,double balance)
        {
            accountID=ID;
            dateOpened=open;
            Balance=balance;
            Console.WriteLine("Chequing Acoount:");
            Console.WriteLine("Account ID:{0}\nDate Opened:{1}\nBalance:{2}", accountID, dateOpened, Balance);
        }
        
        public override void Deposit(double amount)
    {
        Balance=Balance+amount;
        Console.WriteLine("The new balance ={0}", Balance);
    }

    }

    public class TaxFreeSavingsAccount:Account    
    {
    
        public double Limit;

        public TaxFreeSavingsAccount()
        {
            Limit=5000;
        }

        public override void Create(int ID,DateTime open,double balance)
        {
          if (balance <= Limit)
          {
                accountID = ID;
                dateOpened = open;
                Balance = balance;
                Console.WriteLine("Tax free savings account:");
                Console.WriteLine("Account ID:{0}\nDate Opened:{1}\nBalance:{2}\nLimit on account={3}", accountID, dateOpened, Balance, Limit);
         }

         else
            {
                Console.WriteLine("Balance is greater than limit!");
            } 
        }
        
        public override void Deposit(double amount)
    {
        if ((Balance + amount) < 5000)
        {
            Balance = Balance + amount;
            Console.WriteLine("The new balance is={0}", Balance);
        }

        else
            Console.WriteLine("The uuper limit on the account is Rs5000");
    }
    }
}