using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace classes
{
    public class GiftCardAccount : BankAccount
    {
        private decimal _monthlyDeposit = 0m;

        /// <summary>
        /// Creates a new instance of a gift card account type
        /// </summary>
        /// <param name="name"></param>
        /// <param name="initialBalance"></param>
        /// <param name="monthlyDeposit"></param>
        public GiftCardAccount(string name, decimal initialBalance, decimal monthlyDeposit = 0) : base(name, initialBalance) => _monthlyDeposit = monthlyDeposit;

        /// <summary>
        /// Performs gift card account month end transactions, adding a top up ammount as specified
        /// </summary>
        public override void PerformMonthEndTransactions()
        {
            if (_monthlyDeposit != 0)
            {
                MakeDeposit(_monthlyDeposit, DateTime.Now, "Add monthly deposit");
            }
        }
    }
}