using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tushkanchik
{
    public class DepositWithWithdrawal:Deposite
    {

        public bool withdrawal { get; set; }

        public DepositeWithWithdrawal(bool Withdrawal, bool RepLacement, decimal Percent, DateTime StartDate, DateTime FinishDate) 
            : base(RepLacement, Percent, StartDate, FinishDate)

        {
            withdrawal = Withdrawal;
        }
    }
}