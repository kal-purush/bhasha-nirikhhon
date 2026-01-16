using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tushkanchik.Transactions;

namespace Tushkanchik
{
    public abstract class Deposit : CashAccount
    {
        public decimal Percent { get; set; }
        public DateTime StartDate { get; set; }
        public bool Withdrawal { get; set; }
        public Deposit(User holder, decimal balance, string name, decimal percent, DateTime startDate, bool withdrawal)
            : base(holder, balance, name)
        {
            Percent = percent;
            StartDate = startDate;
            Withdrawal = withdrawal;

        }
        public void ChargeInsertOnDeposit(Income income)
        {

        }
    }
}