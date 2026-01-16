using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlexWheels
{
    public class Nets
    {
        public string AccountNumber { get; set; }
        public string TransactionReferenceNo { get; set; }
        public string BankName { get; set; }

        // Constructor
        public Nets(string accountNumber, string transactionReferenceNo, string bankName)
        {
            AccountNumber = accountNumber;
            TransactionReferenceNo = transactionReferenceNo;
            BankName = bankName;
        }
    }
}


