using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankomatConsole
{
    public static class Account
    {
        public static  int AccountId { get; set; }
        public static int AccountBalance { get; set; }
        public static int ShowBalance()
        {
            return AccountBalance;
        }
        public static void IncreaseBalance(int amount)
        {
            AccountBalance += amount;
        }
        public static void DecreaseBalance(int amount)
        {
            AccountBalance -= amount;
        }
    }
}