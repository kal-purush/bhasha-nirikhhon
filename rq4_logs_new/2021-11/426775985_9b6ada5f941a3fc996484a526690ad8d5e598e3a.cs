using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tushkanchik.Transaction
{

    public abstract class Transaction
    {
        public decimal Amount { get; set; }
        public DateTime Date { get; set; }
        //TODO: change int? to Card
        public int? Card { get; set; }

        public string Comment { get; set; }

        public Transaction(decimal amount, DateTime date, int card)
        {
            Amount = amount;
            Date = date;
            Card = card;
        }
    }
}