using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankomatConsole
{
    public static class Operations
    {
        public static double ShowBalance(Account account)
        {
            return account.AccountBalance;
        }
        public static void IncreaseBalance(Account account, double amount)
        {
            account.AccountBalance += amount;
        }
        public static void DecreaseBalance(Account account, double amount)
        {
            account.AccountBalance -= amount;
        }
    }
}