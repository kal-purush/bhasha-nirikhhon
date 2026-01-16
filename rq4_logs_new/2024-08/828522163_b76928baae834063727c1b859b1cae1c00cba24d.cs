using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlexWheels
{
    public class CreditCard
    {
        public string CardNumber { get; set; }
        public string CardHolderName { get; set; }
        public DateTime ExpiryDate { get; set; }
        public int Cvv { get; set; }
        public string BillingAddress { get; set; }

        // Constructor
        public CreditCard(string cardNumber, string cardHolderName, DateTime expiryDate, int cvv, string billingAddress)
        {
            CardNumber = cardNumber;
            CardHolderName = cardHolderName;
            ExpiryDate = expiryDate;
            Cvv = cvv;
            BillingAddress = billingAddress;
        }
    }

}