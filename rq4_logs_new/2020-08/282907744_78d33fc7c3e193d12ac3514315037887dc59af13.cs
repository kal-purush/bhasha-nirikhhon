using System;

namespace Billing.API.Models
{
    public class InvoiceListItem
    {
        public string Product { get; set; }
        public string AccountId { get; set; }
        public DateTime Date { get; set; }
        public string Currency { get; set; }
        public double Amount { get; set; }
        public string Link { get; set; }

        public InvoiceListItem(string product, string accountId, DateTime date, string currency, double amount, string link)
        {
            Product = product;
            AccountId = accountId;
            Date = date;
            Currency = currency;
            Amount = amount;
            Link = link;
        }
    }
}