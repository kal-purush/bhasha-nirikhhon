using System;

namespace Lykke.Service.PayInvoice.Core.Domain.Email
{
    public class PaymentReceivedEmail
    {
        public string MerchantId { get; set; }

        public string EmployeeId { get; set; }

        public string InvoiceNumber { get; set; }

        public string InvoiceId { get; set; }

        public decimal PaidAmount { get; set; }

        public string PaymentAsset { get; set; }

        public DateTime PaidDate { get; set; }

        public string Payer { get; set; }
    }
}