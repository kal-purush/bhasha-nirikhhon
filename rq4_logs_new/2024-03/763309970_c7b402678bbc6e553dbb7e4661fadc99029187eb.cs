using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MVC_Ecommerce.Models
{
    public class PaymentMethod
    {
        public int PaymentMethodId { get; set; }
        public int UserId { get; set; }
        public string CardNumber { get; set; }
        public DateTime ExpiryDate { get; set; }
        public string CVV { get; set; }
        public string CardHolderName { get; set; }
        public int BillingAddressId { get; set; }
    }
}