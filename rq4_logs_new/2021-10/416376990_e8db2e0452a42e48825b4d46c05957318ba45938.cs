using System;

namespace PopLibrary.SqlModels
{
    public class GetPaymentResult
    {
        public int Id { get; set; }
        public int AuctionId { get; set; }
        public int CustomerId { get; set; }
        public bool Complete { get; set; }
        public string StripeInvoiceItemId { get; set; }
        public string StripeInvoiceId { get; set; }
        public DateTime Created { get; set; }
        public decimal Amount { get; set; }
        public string Description { get; set; }
        public string StripeCustomerId { get; set; }
    }
}